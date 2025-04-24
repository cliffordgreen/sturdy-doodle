"""
Task for populating PDF forms with extracted and mapped data.
Uses both PyMuPDF (fitz) and fillpdf (pdftk-based) for filling,
with fillpdf as a fallback when PyMuPDF doesn't work.
"""

from typing import Dict, Any, List
from prefect import task
import os
import shutil

# Import PyMuPDF
try:
    import fitz
    PYMUPDF_AVAILABLE = True
except ImportError:
    print("Warning: PyMuPDF (fitz) library not found. PDF population cannot be performed.")
    PYMUPDF_AVAILABLE = False
    fitz = None

# Import fillpdf as a fallback
try:
    from fillpdf import fillpdfs
    FILLPDF_AVAILABLE = True
except ImportError:
    print("Warning: fillpdf library not found. Fallback PDF population cannot be performed.")
    FILLPDF_AVAILABLE = False
    fillpdfs = None

from utils.helpers import get_pdf_field_mapping # Use helper for mapping

def _flatten_dict(d: Dict[str, Any], parent_key: str = '', sep: str = '_') -> Dict[str, Any]:
    """Flattens a nested dictionary for easier mapping to potentially flat PDF field names."""
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, dict):
            items.extend(_flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list): # Handle lists (e.g., dependents) - basic join
            # This is simplistic; specific handling might be needed based on PDF structure
             items.append((new_key, "; ".join(map(str, v))))
        elif v is not None:
            items.append((new_key, v))
    return dict(items)

def _fill_pdf_pymupdf(template_path: str, output_pdf_path: str, data_for_pdf: Dict[str, Any]):
    """Fills the PDF form using PyMuPDF widget manipulation."""
    if not PYMUPDF_AVAILABLE:
        print("Error: PyMuPDF not available, cannot fill PDF.")
        # Create an empty file as placeholder
        with open(output_pdf_path, 'w') as f:
             f.write("Placeholder - PyMuPDF not available.")
        return

    doc = None
    changes_made = 0
    
    # DEBUGGING - Print the data we're trying to fill
    print(f"DEBUG - Data to fill PDF with: {data_for_pdf}")
    
    try:
        # Make a copy of the template first to avoid modifying the original
        import shutil
        shutil.copy2(template_path, output_pdf_path)
        
        # Open the copy for modification
        doc = fitz.open(output_pdf_path)
        
        # Debug: List all fields in the PDF
        field_list = []
        for page in doc:
            widget = page.first_widget
            while widget:
                field_list.append((widget.field_name, widget.field_type))
                widget = widget.next
        
        print(f"DEBUG - PDF contains {len(field_list)} form fields")
        if field_list:
            print(f"DEBUG - First 5 fields: {field_list[:5]}")
        
        if not doc.needs_pass:
            # Iterate through each page to find and update widgets
            for page in doc:
                widget = page.first_widget
                while widget:
                    field_name = widget.field_name
                    
                    # For debugging - check if this field is in our mapping
                    in_mapping = field_name in data_for_pdf
                    if in_mapping:
                        print(f"DEBUG - Found field to update: {field_name}")
                    
                    if field_name in data_for_pdf:
                        value_to_set = data_for_pdf[field_name]
                        widget_type = widget.field_type
                        widget_type_name = "unknown"
                        
                        # Get readable type name for debugging
                        if widget_type == fitz.PDF_WIDGET_TYPE_TEXT:
                            widget_type_name = "text" 
                        elif widget_type == fitz.PDF_WIDGET_TYPE_CHECKBOX:
                            widget_type_name = "checkbox"
                        elif widget_type == fitz.PDF_WIDGET_TYPE_RADIOBUTTON:
                            widget_type_name = "radiobutton"
                        
                        print(f"DEBUG - Attempting to set {field_name} ({widget_type_name}) to '{value_to_set}'")
                        
                        try:
                            if widget_type == fitz.PDF_WIDGET_TYPE_CHECKBOX:
                                widget.field_value = bool(value_to_set)
                                print(f"DEBUG - Set checkbox {field_name} to {bool(value_to_set)}")
                            elif widget_type == fitz.PDF_WIDGET_TYPE_RADIOBUTTON:
                                if widget.field_name == field_name and str(widget.field_value) == str(value_to_set): # Check specific button
                                     widget.field_value = True
                                     print(f"DEBUG - Set radiobutton {field_name} to True")
                                else:
                                     # Ensure other radio buttons in the same group (same field_name) are off if needed
                                     # This might require more complex group handling - for now, just set the specific one
                                     if bool(value_to_set): # Only attempt to set True
                                          widget.field_value = True
                                          print(f"DEBUG - Set radiobutton {field_name} to True (alternate method)")
                            else: # Default to Text widget handling
                                # Try multiple approaches to set the text value
                                methods_tried = []
                                
                                try:
                                    # Method 1: Try setting field_value directly
                                    methods_tried.append("field_value")
                                    widget.field_value = str(value_to_set)
                                    print(f"DEBUG - Method 1 worked: Set field_value for {field_name}")
                                except Exception as e1:
                                    try:
                                        # Method 2: Try setting text property
                                        methods_tried.append("text property")
                                        widget.text = str(value_to_set)
                                        print(f"DEBUG - Method 2 worked: Set text property for {field_name}")
                                    except Exception as e2:
                                        try:
                                            # Method 3: Try a different interface to widget
                                            methods_tried.append("widget.update_field")
                                            doc.pdf_update_field(widget, str(value_to_set))
                                            print(f"DEBUG - Method 3 worked: Used doc.pdf_update_field for {field_name}")
                                        except Exception as e3:
                                            try:
                                                # Method 4: Try TextWriter as fallback
                                                methods_tried.append("TextWriter")
                                                rect = widget.rect
                                                tw = fitz.TextWriter(page.rect)
                                                tw.append(rect.tl, str(value_to_set))
                                                tw.write_text(page)
                                                print(f"DEBUG - Method 4 worked: Used TextWriter for {field_name}")
                                            except Exception as e4:
                                                print(f"DEBUG - Failed all methods for {field_name}: {methods_tried}")
                                                print(f"DEBUG - Errors: {e1}, {e2}, {e3}, {e4}")
                            
                            # Always try to update the widget after setting values
                            try:
                                widget.update() # Apply the change to the widget
                                changes_made += 1
                                print(f"Successfully set field '{field_name}' to '{value_to_set}'")
                            except Exception as update_error:
                                print(f"DEBUG - Failed to update() widget for {field_name}: {update_error}")
                                
                        except Exception as field_error:
                            print(f"Warning: Failed to set field '{field_name}' (type {widget_type}, value: '{value_to_set}') on page {page.number}: {field_error}")
                    
                    widget = widget.next # Move to the next widget on the page
            
            print(f"Attempted to update {changes_made} fields using PyMuPDF.")
            if changes_made > 0:
                # Close the document
                doc.close()
                print("Document closed after updating fields")
                
                # Open and immediately resave to a new file to avoid any issues
                try:
                    src_doc = fitz.open(output_pdf_path)
                    # Save to temporary file then replace
                    tmp_path = output_pdf_path + ".tmp"
                    src_doc.save(tmp_path, garbage=4, deflate=True, encryption=fitz.PDF_ENCRYPT_NONE)
                    src_doc.close()
                    
                    # Replace original with the temp file
                    import os
                    if os.path.exists(tmp_path):
                        import shutil
                        shutil.move(tmp_path, output_pdf_path)
                        print(f"Successfully saved filled PDF: {output_pdf_path}")
                    else:
                        print(f"Error: Temporary file {tmp_path} not created")
                except Exception as save_error:
                    print(f"Error during final save: {save_error}")
                print(f"Successfully saved filled PDF using PyMuPDF: {output_pdf_path}")
            else:
                # We've already copied the template, no need to save again
                print(f"No fields were updated, but template was copied to: {output_pdf_path}")

        else:
             print(f"Error: PDF {template_path} requires a password.")
             # Create placeholder if password protected
             with open(output_pdf_path, 'w') as f:
                 f.write("Placeholder - PDF requires password.")

    except Exception as e:
        print(f"Error filling PDF with PyMuPDF: {e}")
        # Create a placeholder file with error info
        with open(output_pdf_path, 'w') as f:
             f.write(f"Placeholder for filled PDF - PyMuPDF Error occurred: {e}")
    finally:
         # Don't close doc here since we might have already closed it
         pass

def _fill_pdf_fillpdf(template_path: str, output_pdf_path: str, data_for_pdf: Dict[str, Any]) -> bool:
    """Fills the PDF form using fillpdf (pdftk-based) as a fallback."""
    if not FILLPDF_AVAILABLE:
        print("Error: fillpdf not available, cannot use fallback method.")
        return False
    
    try:
        print(f"Attempting to fill PDF with fillpdf (pdftk) as a fallback method")
        
        # First get the actual field names from the PDF
        try:
            fields_data = fillpdfs.get_form_fields(template_path)
            print(f"Found {len(fields_data)} fields in PDF using fillpdf")
            
            # Print first few fields for debugging
            for i, (field_name, _) in enumerate(list(fields_data.items())[:5]):
                print(f"Field {i+1}: {field_name}")
            
            # Check for field name format mismatch - fillpdf may return fields with different encoding
            if not any(key in fields_data for key in data_for_pdf.keys()):
                print("Warning: Field name format mismatch between mapping and actual PDF fields")
                
                # Try to match field names by looking for patterns
                adjusted_data = {}
                for pdf_field in fields_data.keys():
                    for mapped_field, value in data_for_pdf.items():
                        # Try to extract the basic field ID (like f1_01)
                        if mapped_field and pdf_field:
                            field_id = None
                            
                            # Extract field ID from mapped name like "topmostSubform[0].Page1[0].f1_01[0]"
                            mapped_parts = mapped_field.split('.')
                            for part in mapped_parts:
                                if part.startswith('f1_') or part.startswith('f2_'):
                                    field_id = part.split('[')[0]
                                    break
                            
                            # If we found an ID and it exists in the PDF field name, use it
                            if field_id and field_id in pdf_field:
                                adjusted_data[pdf_field] = value
                                print(f"Matched {mapped_field} to {pdf_field} via pattern {field_id}")
                
                if adjusted_data:
                    print(f"Adjusted {len(adjusted_data)} fields for fillpdf format")
                    data_for_pdf = adjusted_data
        except Exception as e:
            print(f"Error getting form fields with fillpdf: {e}")
            # Continue with original mapping as fallback
        
        # Fill and save the PDF - use flattening to make fields visible in all viewers
        fillpdfs.write_fillable_pdf(template_path, output_pdf_path, data_for_pdf, flatten=True)
        print(f"Successfully filled PDF using fillpdf (flattened) to {output_pdf_path}")
        return True
        
    except Exception as e:
        print(f"Error filling PDF with fillpdf: {e}")
        import traceback
        traceback.print_exc()
        return False

@task
def populate_pdf_form(mapped_gemini_data: Dict[str, Any], template_path: str, output_pdf_path: str) -> str:
    """Prefect task to fill a PDF form template using PyMuPDF with fillpdf fallback.
       Accepts a flat dictionary keyed by Gemini field names.
    """
    print(f"Starting PDF population for template: {template_path}")

    # Check for upstream errors passed via mapped_gemini_data
    if mapped_gemini_data.get("error"):
        error_msg = mapped_gemini_data["error"]
        print(f"Skipping population due to upstream error: {error_msg}")
        with open(output_pdf_path, 'w') as f:
            f.write(f"Placeholder - Upstream error: {error_msg}")
        return output_pdf_path 

    # Check if there's actual data (ignoring potential metadata)
    data_to_fill_check = {k: v for k, v in mapped_gemini_data.items() if not k.startswith("_")}
    if not data_to_fill_check:
        print("Warning: No actual data fields provided from mapping. Skipping population.")
        with open(output_pdf_path, 'w') as f:
            f.write("Placeholder - No data provided for population.")
        return output_pdf_path

    # Data is already flat, keyed by Gemini field names
    data_to_fill = data_to_fill_check
    print(f"DEBUG - Data received (Gemini keys): {data_to_fill}")

    # --- NEW: Map Gemini field names to actual PDF technical field names --- 
    # TODO: Move this to a configuration file (e.g., mappings/1040_gemini_to_pdf.json)
    # Technical names based on previous logs / PDF inspection
    GEMINI_TO_PDF_MAP = {
        # Personal Info
        "FirstNameInitial": "topmostSubform[0].Page1[0].f1_01[0]", 
        "LastName": "topmostSubform[0].Page1[0].f1_02[0]",
        "YourSocialSecurityNumber": "topmostSubform[0].Page1[0].f1_03[0]",
        # Income
        "Income_1z": "topmostSubform[0].Page1[0].Line1-8c_ReadOrder[0].f1_25[0]", # Corresponds to Line 1z field
        # Payments (Page 2)
        "Line25a_FormW2": "topmostSubform[0].Page2[0].f2_09[0]", # Placeholder - actual name needed!
        # Add other mappings here as needed...
    }
    print(f"DEBUG - Using Gemini->PDF map: {GEMINI_TO_PDF_MAP}")

    # Prepare the final dictionary using the mapped PDF field names
    final_data_for_pdf = {}
    mapped_count = 0
    for gemini_key, value in data_to_fill.items():
        if gemini_key in GEMINI_TO_PDF_MAP:
            pdf_field_name = GEMINI_TO_PDF_MAP[gemini_key]
            # Apply basic normalization based on Gemini field type if needed?
            # For now, just convert to string for PDF fields
            final_data_for_pdf[pdf_field_name] = str(value) 
            mapped_count += 1
            print(f"DEBUG - Prepared mapping: {pdf_field_name} = '{str(value)}'")
        else:
            print(f"Warning: No PDF field mapping found for Gemini key: {gemini_key}")

    if mapped_count == 0:
        print("Error: No provided data fields could be mapped to known PDF fields. Cannot populate form.")
        with open(output_pdf_path, 'w') as f:
            f.write("Placeholder - No data mapped to PDF fields.")
        return output_pdf_path

    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_pdf_path), exist_ok=True)

    # First try PyMuPDF
    print("Attempting to fill PDF with PyMuPDF")
    pymupdf_output_path = output_pdf_path + ".pymupdf.pdf"
    # Pass data keyed by technical PDF names
    _fill_pdf_pymupdf(template_path, pymupdf_output_path, final_data_for_pdf)
    
    # Also try fillpdf, which often works better for certain PDFs
    print("Also trying fillpdf as a backup approach")
    fillpdf_output_path = output_pdf_path + ".fillpdf.pdf"
    # Pass data keyed by technical PDF names
    fillpdf_success = _fill_pdf_fillpdf(template_path, fillpdf_output_path, final_data_for_pdf)
    
    # Copy the best result to the output path
    # For now, prefer fillpdf if it succeeded
    if fillpdf_success and os.path.exists(fillpdf_output_path) and os.path.getsize(fillpdf_output_path) > 1000:
        shutil.copy(fillpdf_output_path, output_pdf_path)
        print(f"Using fillpdf result as final output: {output_pdf_path}")
    elif os.path.exists(pymupdf_output_path) and os.path.getsize(pymupdf_output_path) > 1000:
        shutil.copy(pymupdf_output_path, output_pdf_path)
        print(f"Using PyMuPDF result as final output: {output_pdf_path}")
    else:
        # If neither worked, create an empty file
        with open(output_pdf_path, 'w') as f:
            f.write("Placeholder - Both PDF filling methods failed.")
    
    # Clean up temporary files
    for temp_file in [pymupdf_output_path, fillpdf_output_path]:
        if os.path.exists(temp_file):
            try:
                os.remove(temp_file)
            except:
                pass

    print(f"PDF population task finished. Output: {output_pdf_path}")
    return output_pdf_path 