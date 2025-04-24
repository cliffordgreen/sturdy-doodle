"""
Task for reviewing a populated structure and using Gemini to infer missing values.
"""
import os
import json
from typing import Dict, Any
from prefect import task, get_run_logger

# Assuming Gemini setup is available
try:
    import google.generativeai as genai
    from google.api_core.exceptions import GoogleAPIError
    # TODO: Add configuration for API key (e.g., from environment variable)
    # genai.configure(api_key=os.environ["GEMINI_API_KEY"]) 
    GEMINI_AVAILABLE = True 
except ImportError:
    print("Warning: Gemini library not found. Document review cannot be performed.")
    GEMINI_AVAILABLE = False
    genai = None
    GoogleAPIError = None

def _call_gemini_for_review(populated_context: str, unpopulated_context: str, target_form: str) -> Dict[str, str]:
    """Calls Gemini API to infer missing values based on populated context."""
    if not genai or not GEMINI_AVAILABLE:
        return {} # Cannot infer without Gemini

    logger = get_run_logger()
    model = genai.GenerativeModel('gemini-2.5-flash-preview-04-17') # Updated model

    # Define specific calculations to check based on form type
    validation_checks = ""
    if target_form == '1040':
        validation_checks = """
        - Verify Line 9 (Total Income): Does it correctly sum lines 1z, 2b, 3b, 4b, 5b, 6b, 7, and 8 based on the provided values?
        - Verify Line 11 (Adjusted Gross Income): Does it equal Line 9 minus Line 10?
        """
    # Add elif blocks for SchedC, SchedE, 1040-SE etc. as calculations are implemented
    elif target_form == 'SchedC':
        validation_checks = """
        - Verify Line 3 (Gross Profit): Does it equal Line 1 minus Line 2?
        - Verify Line 5 (Gross Income): Does it equal Line 3 minus Line 4?
        - Verify Line 28 (Total Expenses): Does it sum lines 8 through 27a?
        - Verify Line 31 (Net profit or loss): Does it equal Line 5 minus Line 28?
        """

    prompt = f"""
You are an expert tax form reviewer analyzing a partially populated data structure for IRS Form {target_form}.
Your goals are to:
1. Infer plausible values for empty fields based ONLY on populated related fields.
2. Validate specific calculations based on standard IRS rules for this form.

Context - Populated Fields:
```json
{populated_context}
```

Task 1: Infer Values for Empty Fields
Based *only* on the populated fields context above, can you deduce the values for any of the following empty fields? 
Consider relationships like a full address containing city/state/zip, or a full name containing first/last names.

Empty Fields to Consider:
```json
{unpopulated_context}
```

Task 2: Validate Calculations
Based on the populated fields context and standard IRS rules for Form {target_form}, check the following calculations. Note any discrepancies.
{validation_checks}

Output Format:
Return ONLY a single JSON object with two keys: 'inferred_values' and 'validation_errors'.
- 'inferred_values': A JSON object mapping the 'field_name' of any empty fields you could confidently infer to their inferred 'value'. Only include fields where the value can be directly extracted or logically deduced from the populated context. Do not guess or make up information. If none, use {{}}.
- 'validation_errors': A list of strings, where each string describes a specific calculation discrepancy found during Task 2. If no errors are found, use an empty list [].

Example Output:
{{
  "inferred_values": {{
    "City": "Anytown",
    "State": "CA",
    "ZipCode": "90210"
  }},
  "validation_errors": [
    "Line 9 (Total Income) calculation appears incorrect based on provided components."
  ]
}}
"""

    max_retries = 3
    for attempt in range(max_retries):
        try:
            logger.info(f"Calling Gemini for review/inference (Attempt {attempt + 1}/{max_retries})...")
            response = model.generate_content(prompt)
            response.resolve()
            
            raw_text = response.text.strip()
            # Basic JSON extraction (might need refinement)
            json_text = raw_text
            if raw_text.startswith("```json"):
                json_text = raw_text[7:]
            if raw_text.endswith("```"):
                json_text = json_text[:-3]
            json_text = json_text.strip()
            
            if not json_text.startswith("{") or not json_text.endswith("}"):
                # Handle cases where Gemini might return non-JSON or just text like "None"
                if json_text.lower() == "none" or not json_text:
                     logger.info("Gemini review indicated no values could be inferred.")
                     return {}
                else:
                     logger.warning(f"Gemini review response is not a valid JSON object: {raw_text}")
                     # Attempt to extract if possible
                     json_start = raw_text.find('{')
                     json_end = raw_text.rfind('}') + 1
                     if json_start != -1 and json_end > json_start:
                          json_text = raw_text[json_start:json_end]
                          print("Attempting to parse extracted JSON substring from review.")
                     else:
                          return {} # Cannot parse

            parsed_response = json.loads(json_text)
            if isinstance(parsed_response, dict) and \
               'inferred_values' in parsed_response and \
               'validation_errors' in parsed_response and \
               isinstance(parsed_response['inferred_values'], dict) and \
               isinstance(parsed_response['validation_errors'], list):
                
                inferred_count = len(parsed_response['inferred_values'])
                error_count = len(parsed_response['validation_errors'])
                logger.info(f"Gemini review inferred {inferred_count} values and found {error_count} validation errors.")
                if error_count > 0:
                    for err in parsed_response['validation_errors']:
                        logger.warning(f"Gemini Validation Error: {err}")
                       
                # Return the whole parsed structure
                return parsed_response
            else:
                 logger.warning(f"Gemini review returned JSON but with unexpected structure/types: {parsed_response}")
                 return {"inferred_values": {}, "validation_errors": ["Review response parsing failed"]} # Return default structure on error

        except json.JSONDecodeError as json_err:
            logger.error(f"Error decoding JSON from Gemini review response: {json_err}")
            print(f"Raw review response text:\n{raw_text}")
            # Return default structure on error
            return {"inferred_values": {}, "validation_errors": [f"JSON Decode Error: {json_err}"]}
        except GoogleAPIError as e:
             logger.error(f"Gemini API error during review (Attempt {attempt + 1}): {e}")
             if attempt == max_retries - 1: 
                 return {"inferred_values": {}, "validation_errors": [f"Gemini API Error after retries: {e}"]}
        except Exception as e:
            logger.error(f"Non-API error during Gemini review (Attempt {attempt + 1}): {e}")
            # Return default structure on error
            return {"inferred_values": {}, "validation_errors": [f"Unexpected Review Error: {e}"]}
            
    # Return default structure if all retries fail
    return {"inferred_values": {}, "validation_errors": ["Gemini call failed after max retries"]}

@task
def review_and_repopulate_with_gemini(populated_structure: Dict[str, Any], target_form: str) -> Dict[str, Any]:
    """
    Reviews a populated structure, uses Gemini to infer missing values based on context,
    and updates the structure with inferred values and flags.
    """
    logger = get_run_logger()
    logger.info(f"Starting Gemini review pass for {target_form} structure.")

    if not GEMINI_AVAILABLE:
        logger.warning("Gemini not available, skipping review pass.")
        return populated_structure
        
    # 1. Prepare Contexts for Prompt
    populated_fields_context = {}
    unpopulated_fields_context = []
    original_field_definitions = {} # Keep original definition for update

    for page_key, page_content in populated_structure.items():
        if isinstance(page_content, dict) and "fields" in page_content and isinstance(page_content["fields"], list):
            for field_def in page_content["fields"]:
                if isinstance(field_def, dict) and "field_name" in field_def:
                    fname = field_def["field_name"]
                    original_field_definitions[fname] = field_def # Store reference
                    if "value" in field_def and field_def["value"] is not None:
                        populated_fields_context[fname] = field_def["value"]
                    else:
                        unpopulated_fields_context.append({
                            "field_name": fname,
                            "label_text": field_def.get("label_text"),
                            "location_hint": field_def.get("location_hint")
                        })
    
    if not unpopulated_fields_context:
        logger.info("No unpopulated fields found. Skipping Gemini review call.")
        return populated_structure
        
    if not populated_fields_context:
        logger.info("No populated fields found to provide context. Skipping Gemini review call.")
        return populated_structure

    # Convert contexts to JSON strings for the prompt
    try:
        populated_context_json = json.dumps(populated_fields_context, indent=2)
        unpopulated_context_json = json.dumps(unpopulated_fields_context, indent=2)
    except TypeError as e:
         logger.error(f"Failed to serialize context for Gemini prompt: {e}")
         return populated_structure # Cannot proceed

    # 2. Call Gemini for Review and Validation
    review_results = _call_gemini_for_review(populated_context_json, unpopulated_context_json, target_form)
    inferred_values = review_results.get('inferred_values', {})
    validation_errors = review_results.get('validation_errors', [])

    # Add validation errors to the structure for downstream awareness or reporting
    if validation_errors:
        populated_structure["_validation_errors"] = validation_errors
        # Potentially modify flow status based on these errors later

    # 3. Merge Inferred Values and Add Flags
    update_count = 0
    if inferred_values: # Check if Gemini returned any inferences
        for field_name, inferred_value in inferred_values.items():
            if field_name in original_field_definitions:
                field_def = original_field_definitions[field_name]
                # Only update if the field doesn't already have a value
                if "value" not in field_def or field_def["value"] is None:
                     cleaned_inferred = str(inferred_value).strip() # Basic cleaning
                     if cleaned_inferred: # Don't add empty strings
                         field_def["value"] = cleaned_inferred
                         field_def["population_method"] = "GeminiReviewPass"
                         field_def["sources"] = ["Inferred from populated fields by GeminiReviewPass"] # Overwrite/set sources
                         logger.info(f"GeminiReviewPass populated {field_name} with value: '{cleaned_inferred}'")
                         update_count += 1
                else:
                     logger.warning(f"GeminiReviewPass tried to populate {field_name} but it already had value: {field_def['value']}")
            else:
                logger.warning(f"GeminiReviewPass returned value for unknown field_name: {field_name}")
                
    logger.info(f"Gemini Review Pass complete. Updated {update_count} fields. Found {len(validation_errors)} validation issues.")
    return populated_structure 