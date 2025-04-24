"""
Main Prefect flow for automating tax form population.
Orchestrates the ingestion, extraction, mapping, population, and validation tasks.
"""

import os
import json
import re
import glob # Import glob
from collections import Counter # Import Counter
from typing import Dict, Any, List
from prefect import flow, get_run_logger
from pathlib import Path

# Import tasks from their respective modules
from tasks.ingestion import ingest_and_preprocess
# REMOVE OLD EXTRACTION IMPORTS
# from tasks.extraction import extract_text_layout
# from tasks.information_extraction import extract_information
from tasks.mapping import create_populated_gemini_structure
from tasks.population import populate_pdf_form
from tasks.validation import validate_form
from tasks.classification import classify_document # Import the new task
from tasks.review import review_and_repopulate_with_gemini # Import the new review task

# Import the NEW source document data extractor
from data_extraction.source_document_extractor import extract_data_from_source_pdf

# Import helpers
from utils.helpers import load_schema, load_validation_rules, determine_target_forms

# --- Configuration --- 
# These paths should be relative to the project root or use absolute paths
SCHEMA_DIR = "schemas"
RULES_DIR = "rules"
TEMPLATE_DIR = "templates"
# Change OUTPUT_DIR to the location of the generated schemas
OUTPUT_DIR = "output" # Gemini extractor saves here by default
GENERATED_SCHEMA_DIR = "output" # Explicitly define where Gemini schemas are

# Define path for the Gemini blank fields structure file
GEMINI_BLANK_FIELDS = {
    '1040': os.path.join(OUTPUT_DIR, 'f1040_blank_gemini_extracted_fields.json'),
    'SchedC': os.path.join(OUTPUT_DIR, 'f1040sc_blank_gemini_extracted_fields.json'),
    'SchedE': os.path.join(OUTPUT_DIR, 'f1040se_gemini_extracted_fields.json'), # Path to the Sched E structure file (misnamed)
    '1040-SE': os.path.join(OUTPUT_DIR, 'f1040se_gemini_extracted_fields.json'), # Assuming SE uses same base name for fields for now
    'Schedule 1': os.path.join(OUTPUT_DIR, 'f1040s1_blank_gemini_extracted_fields.json'), # Assumed path for Sched 1
    'Schedule 2': os.path.join(OUTPUT_DIR, 'f1040s2_blank_gemini_extracted_fields.json'), # Assumed path for Sched 2
    'Schedule 3': os.path.join(OUTPUT_DIR, 'f1040s3_blank_gemini_extracted_fields.json'), # Assumed path for Sched 3
    'Form 2441': os.path.join(OUTPUT_DIR, 'f2441_blank_gemini_extracted_fields.json'), # Assumed path for Form 2441
    'Form 8812': os.path.join(OUTPUT_DIR, 'f8812_blank_gemini_extracted_fields.json'), # Assumed path for Form 8812
    'Schedule A': os.path.join(OUTPUT_DIR, 'f1040sa_blank_gemini_extracted_fields.json') # Assumed path for Sched A
}

# Load static configurations once
# FORM_SCHEMAS = {
#     # Schema loading is commented out as it's not used in the current Gemini-field-driven approach
#     # '1040': load_schema(os.path.join(SCHEMA_DIR, '1040.json')), 
#     # 'SchedC': load_schema(os.path.join(SCHEMA_DIR, 'SchedC.json')) 
# }
VALIDATION_RULES = {
    '1040': load_validation_rules(os.path.join(RULES_DIR, '1040_validation.py')),
    'SchedC': load_validation_rules(os.path.join(RULES_DIR, 'SchedC_validation.py'))
}
PDF_TEMPLATES = {
    '1040': os.path.join(TEMPLATE_DIR, 'f1040_blank.pdf'),
    'SchedC': os.path.join(TEMPLATE_DIR, 'f1040sc_blank.pdf')
}

os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- Flow Definition --- 

@flow(log_prints=True)
def tax_form_automation_flow(source_document_paths: List[str]):
    """Main Prefect flow to process source documents and populate target tax forms."""
    logger = get_run_logger()
    logger.info(f"Starting tax automation flow for source documents: {source_document_paths}")

    # --- 1. Input Validation & Pre-classification --- 
    valid_paths = []
    classification_futures = []
    path_to_filename = {}
    for path in source_document_paths:
        if not os.path.exists(path):
            logger.error(f"Input source document not found: {path}. Skipping this file.")
        else:
            valid_paths.append(path)
            filename = Path(path).name
            path_to_filename[path] = filename
            # Submit classification task for each valid path
            # Use .submit() for potential parallelism if using a runner that supports it
            logger.info(f"Submitting classification task for: {filename}")
            classification_futures.append(classify_document.submit(doc_path=path, doc_filename=filename))

    if not valid_paths:
        logger.error("No valid source document paths found. Aborting flow.")
        return { "status": "FAILED", "error": "No valid input files provided." }

    # Wait for classifications to complete and build mapping
    doc_path_to_type = {}
    logger.info(f"Waiting for {len(classification_futures)} classification(s) to complete...")
    for i, future in enumerate(classification_futures):
        try:
            # Use .result() with a timeout if needed
            doc_type = future.result() 
            doc_path = valid_paths[i] # Assumes results maintain order
            doc_path_to_type[doc_path] = doc_type
            logger.info(f"Classified {path_to_filename[doc_path]} as: {doc_type}")
        except Exception as e:
             doc_path = valid_paths[i]
             logger.error(f"Classification failed for {path_to_filename[doc_path]}: {e}")
             doc_path_to_type[doc_path] = "Other" # Default type on error
    logger.info("Document classification finished.")

    # --- 2. Gemini Data Extraction & Grouped Aggregation ---
    # aggregated_data = {} # Old flat structure
    aggregated_data_by_type = {} # NEW: Dictionary of lists keyed by doc type
    extraction_errors = {}
    logger.info(f"Starting Gemini data extraction for {len(valid_paths)} source document(s)...")

    # --- REMOVE old aggregation setup (summable_keys etc.) --- 
    # summable_keys = {...}
    # list_keys = {...}
    # modal_keys = {...}
    # all_values_for_modal_keys = {}

    for doc_path in valid_paths:
        doc_type = doc_path_to_type.get(doc_path, "Other") # Get pre-classified type
        filename = path_to_filename.get(doc_path, doc_path)
        logger.info(f"Extracting data from: {filename} (Type: {doc_type})")
        try:
            extracted_page_data = extract_data_from_source_pdf(pdf_path=doc_path, doc_type=doc_type)
            
            # Combine all pages for the *current* document AND add source info
            current_document_data_with_source = {}
            for page_num, page_data in extracted_page_data.items():
                if isinstance(page_data, dict) and 'error' not in page_data:
                     for key, value in page_data.items():
                         # Store value along with its source path
                         # Overwrite if key appears on multiple pages of the SAME document (take last page's value)
                         current_document_data_with_source[key] = {"value": value, "source": doc_path} 
                elif isinstance(page_data, dict) and 'error' in page_data:
                    # Log page-level errors, potentially store them if needed
                    logger.warning(f"Error extracting page {page_num} from {filename}: {page_data['error']}")
                    error_key = f"{filename}_page_{page_num}"
                    extraction_errors[error_key] = page_data['error']
            
            # Append the combined data (with source info) for this document to the appropriate list
            if current_document_data_with_source: # Only append if we got some data
                aggregated_data_by_type.setdefault(doc_type, []).append(current_document_data_with_source)
                logger.info(f"Appended data with source info from {filename} to category '{doc_type}'")
            else:
                 logger.warning(f"No data extracted or only errors found for {filename}. Skipping append.")

        except Exception as e:
            logger.error(f"Failed processing source document {filename}: {e}", exc_info=True)
            extraction_errors[doc_path] = f"Outer extraction loop error: {e}"

    # --- Aggregation logic (Sum/Mode/List) is now deferred to the downstream task --- 

    # --- Aggregation finished (Data is now grouped by type, with source info) --- 

    if not aggregated_data_by_type and extraction_errors:
        logger.error("Data extraction failed for all documents or produced no data.")
        return { "status": "FAILED", "error": "Data extraction failed", "details": extraction_errors }

    # Optionally save the *new* aggregated data structure
    try:
        output_base = Path(valid_paths[0]).stem # Still use first doc for base filename
        agg_data_filename = os.path.join(OUTPUT_DIR, f"_DEBUG_aggregated_data_by_type_{output_base}.json")
        with open(agg_data_filename, 'w') as f:
            json.dump(aggregated_data_by_type, f, indent=2, default=str)
        logger.info(f"DEBUG: Saved aggregated data (grouped by type) to: {agg_data_filename}")
    except Exception as save_err:
        logger.error(f"DEBUG: Failed to save grouped aggregated data: {save_err}")

    # --- 3. Determine Target Forms --- 
    # NOTE: This might need adjustment based on the new aggregated_data_by_type structure.
    # For now, it might still work if it checks for *any* presence of indicator keys
    # across all document types, but a more targeted check might be better later.
    try:
        # Let's temporarily flatten keys for determine_target_forms for compatibility
        temp_flat_aggregated = {}
        for doc_list in aggregated_data_by_type.values():
             for doc_data in doc_list:
                  temp_flat_aggregated.update(doc_data)
        target_forms = determine_target_forms(temp_flat_aggregated) 
        # --- Add logic to include 1040-SE if SchedC is present --- 
        if 'SchedC' in target_forms:
            if '1040-SE' not in target_forms:
                logger.info("Schedule C present, adding 1040-SE to target forms.")
                # Note: 1040-SE processing hasn't been fully added/tested yet
                # target_forms.append('1040-SE') 
        # --- Add logic to include SchedE if Cash Flow Statement data exists --- 
        if "Cash Flow Statement" in aggregated_data_by_type:
             if 'SchedE' not in target_forms:
                  logger.info("Cash Flow Statement data found, adding SchedE to target forms.")
                  target_forms.append('SchedE')
        # --- End SchedE logic ---
        logger.info(f"Final target forms identified (incl. auto-add logic): {target_forms}")
    except Exception as e:
        logger.error(f"Failed during target form determination: {e}", exc_info=True)
        return { "status": "FAILED", "error": f"Target Form Determination Error: {e}" }

    # --- Define Processing Order --- 
    # Ensure schedules/forms are processed before forms that depend on them
    PROCESSING_ORDER = [
        'SchedC',
        'SchedE', 
        # Add Sched F if implemented
        '1040-SE', # Depends on SchedC
        'Schedule 1', # Depends on Sched C/E/F/SE
        # Add other forms feeding Sched 2 (e.g., 6251, 8962, 5329, Sch H, 8959, 8960)
        'Schedule 2', # Depends on 1040-SE and others
        # Add other forms feeding Sched 3 (e.g., 1116, 8863, 8880, 5695, 8962)
        'Form 2441', # Feeds Sched 3 and 1040
        'Schedule 3', # Depends on Form 2441 etc.
        'Form 8812', # Depends on AGI (Sch 1), Feeds 1040 Lines 19 & 28
        'Schedule A', # Depends on AGI (Sch 1), Feeds 1040 Line 12
        '1040'     # Main form processed last
    ]

    # Create the ordered list based on determined targets
    ordered_target_forms = [form for form in PROCESSING_ORDER if form in target_forms]
    # Add any determined targets that weren't in the predefined order (should be rare, log warning if needed)
    additional_targets = [form for form in target_forms if form not in ordered_target_forms]
    if additional_targets:
        logger.warning(f"Found target forms not in predefined PROCESSING_ORDER: {additional_targets}. Appending them.")
        ordered_target_forms.extend(additional_targets)
    
    logger.info(f"Processing target forms in order: {ordered_target_forms}")

    # --- Process Each Target Form in Order --- 
    # NOTE: The mapping task (`create_populated_gemini_structure`) now handles aggregation and calculation
    results = {}
    populated_structures_cache = {} # Cache populated structures for dependencies
    flow_status = "COMPLETED" 
    if extraction_errors:
        flow_status = "COMPLETED_WITH_EXTRACTION_ERRORS"

    # Remove temporary flattening of aggregated data - no longer needed
    # temp_flat_aggregated = {}
    # for doc_list in aggregated_data_by_type.values():
    #     for doc_data in doc_list:
    #          temp_flat_aggregated.update(doc_data)

    for form_type in ordered_target_forms: # Iterate through the ordered list
        logger.info(f"--- Processing target form type: {form_type} ---")
        gemini_blank_fields_path = GEMINI_BLANK_FIELDS.get(form_type)

        if not gemini_blank_fields_path or not os.path.exists(gemini_blank_fields_path):
            logger.error(f"Gemini blank fields file not found or configured for form type '{form_type}' at {gemini_blank_fields_path}. Skipping.")
            results[form_type] = {"status": "SKIPPED", "error": f"Missing/Invalid Gemini blank fields file path for {form_type}"}
            flow_status = "COMPLETED_WITH_SKIPS"
            continue

        output_base = Path(valid_paths[0]).stem
        output_json_filename = f"{output_base}_{form_type}_populated_gemini_structure.json"
        output_json_path = os.path.join(OUTPUT_DIR, output_json_filename)

        try:
            # --- 4. Map data, Aggregate, Calculate, and Create Populated Gemini Structure --- 
            # Pass the original grouped data and the cache of already processed forms
            populated_structure_initial = create_populated_gemini_structure(
                aggregated_data_by_type, 
                form_type,
                gemini_blank_fields_path,
                populated_structures_cache # Pass cache for dependency lookups
            )

            # === DEBUGGING: Check the output of the mapping/calculation task ===
            logger.info(f"DEBUG: Value after create_populated_gemini_structure ({form_type}) (first 500 chars): {str(populated_structure_initial)[:500]}...")
            # === END DEBUGGING ===

            # Check if the initial task returned an error
            if isinstance(populated_structure_initial, dict) and populated_structure_initial.get("error"):
                 raise Exception(f"Task create_populated_gemini_structure failed for {form_type}: {populated_structure_initial['error']}")

            # --- 4.5 Gemini Review and Repopulation Pass --- 
            logger.info(f"Starting Gemini review pass for {form_type}...")
            populated_structure_final = review_and_repopulate_with_gemini(
                populated_structure_initial, # Pass the structure from the previous step
                form_type
            )

            # === DEBUGGING: Check the output of the review task ===
            logger.info(f"DEBUG: Value of populated_structure_final after review ({form_type}) (first 500 chars): {str(populated_structure_final)[:500]}...")
            # === END DEBUGGING ===
            
            # Check if the review task returned an error
            if isinstance(populated_structure_final, dict) and populated_structure_final.get("error"): # Check standard error key
                 logger.warning(f"Gemini review task for {form_type} encountered an error: {populated_structure_final['error']}")
                 # Decide whether to proceed or raise. For now, we proceed but log warning.
                 # Update flow status to indicate review issues potentially requiring manual check
                 if flow_status == "COMPLETED": flow_status = "COMPLETED_WITH_REVIEW_ERRORS" 
                 elif flow_status == "COMPLETED_WITH_EXTRACTION_ERRORS": flow_status = "COMPLETED_WITH_EXTRACTION_AND_REVIEW_ERRORS"
                 # Add other combinations if needed

            # --- Cache and Save the FINAL Populated Structure --- 
            populated_structures_cache[form_type] = populated_structure_final # Update cache with final version AFTER review
            try:
                with open(output_json_path, 'w') as f:
                    json.dump(populated_structure_final, f, indent=2) # Save the result of the review task
                logger.info(f"Successfully saved FINAL populated Gemini structure to: {output_json_path}")
                results[form_type] = {"status": "PROCESSED", "populated_structure_path": output_json_path}
            except Exception as save_e:
                 logger.error(f"Failed to save final populated structure to {output_json_path}: {save_e}")
                 # Re-raise the exception after logging to ensure the flow knows about the critical save error
                 raise save_e 
        except Exception as e:
            logger.error(f"Error processing target form {form_type} using data from {valid_paths}: {e}", exc_info=True)
            results[form_type] = {"status": "FAILED", "error": str(e)}
            flow_status = "FAILED" # If one form fails critically, mark the whole flow as failed
            # Decide if one form failure stops all? break if so.
            # For now, we continue processing other forms but the overall status is FAILED.

    logger.info(f"Flow finished for source documents: {source_document_paths}. Status: {flow_status}")
    final_result = {"status": flow_status, "results_per_form": results, "extraction_errors": extraction_errors}

    # Save final results summary
    # Determine a base name for the summary file (e.g., from the first doc or a timestamp)
    summary_base_name = "flow_summary"
    if valid_paths:
        summary_base_name = f"{Path(valid_paths[0]).stem}_summary"
        
    summary_filename = f"{summary_base_name}.json"
    summary_path = os.path.join(OUTPUT_DIR, summary_filename)
    try:
        with open(summary_path, 'w') as f:
            json.dump(final_result, f, indent=2)
        logger.info(f"Saved final summary report to: {summary_path}")
    except Exception as e:
        logger.error(f"Could not save final summary report: {e}")

    return final_result

# --- Example Trigger ---
if __name__ == "__main__":
    # Example: Provide a list of paths to your source documents (W2, 1099, receipts etc.)
    # Ensure these files exist or adapt the paths.
    # OLD: example_input_paths = [
    #    "tax_documents/illumina-w2-2019.pdf", # Updated path
    #    # "tax_documents/example_1099int.pdf",
    #    # "tax_documents/example_receipt.pdf"
    #    # Add more paths as needed
    # ]
    
    # NEW: Dynamically find all PDF files in the tax_documents directory
    tax_docs_dir = "tax_documents"
    example_input_paths = glob.glob(os.path.join(tax_docs_dir, "*.pdf"))
    # Optional: Add specific non-PDFs if needed, or filter further
    # example_input_paths.extend(glob.glob(os.path.join(tax_docs_dir, "*.png"))) # Example
    
    print(f"Found input documents: {example_input_paths}")

    # Check if dummy file exists, create if needed (for testing purposes)
    # Commenting out dummy file creation as we are using a real file
    # if example_input_paths and not os.path.exists(example_input_paths[0]):
    #      # Create a dummy W2-like file if the primary example doesn't exist
    #      dummy_dir = os.path.dirname(example_input_paths[0])
    #      os.makedirs(dummy_dir, exist_ok=True)
    #      print(f"Creating dummy file: {example_input_paths[0]}")
    #      # Cannot create a real PDF easily, create a txt file as placeholder
    #      with open(example_input_paths[0].replace('.pdf', '.txt'), "w") as f:
    #          f.write("This is a dummy file placeholder.")
    #          # In reality, use actual PDF source documents for testing.

    # Run the flow with the list of source document paths
    if example_input_paths:
        # Ensure the file exists before running
        if os.path.exists(example_input_paths[0]):
             tax_form_automation_flow(example_input_paths)
        else:
             print(f"Error: Input file not found at {example_input_paths[0]}. Please check the path.")
    else:
        print("Please update 'example_input_paths' in main_flow.py with paths to your source documents.") 