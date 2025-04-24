# pdf_extraction/gemini_extractor.py
import os
import google.generativeai as genai
from pdf2image import convert_from_path
from PIL import Image
import json
import io
from dotenv import load_dotenv
from typing import List, Dict, Any
import time # Added for potential delay
import sys # Import sys for command-line arguments

# Load API key from .env file
load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

if not GEMINI_API_KEY:
    raise ValueError("GEMINI_API_KEY not found in environment variables. Please set it in a .env file.")

genai.configure(api_key=GEMINI_API_KEY)

# Initialize the Gemini Pro Vision model
# Adjust model name if needed, e.g., 'gemini-1.5-pro-latest'
# model = genai.GenerativeModel('gemini-2.0-flash')
model = genai.GenerativeModel('gemini-2.5-flash-preview-04-17') # Updated model

def pdf_to_images(pdf_path: str) -> List[Image.Image]:
    """Converts a PDF file into a list of PIL Image objects."""
    try:
        # Ensure poppler is in PATH or specify poppler_path
        # Example for specifying path if not in system PATH (adjust as needed):
        # images = convert_from_path(pdf_path, poppler_path=r"C:\path\to\poppler\bin")
        images = convert_from_path(pdf_path)
        return images
    except Exception as e:
        print(f"Error converting PDF to images: {e}")
        print("Ensure poppler is installed and in your system's PATH.")
        # Installation instructions:
        # On macOS: brew install poppler
        # On Debian/Ubuntu: sudo apt-get install -y poppler-utils
        # On Windows: Download from official site/conda-forge and add bin directory to PATH
        raise

def image_to_byte_array(image: Image.Image) -> bytes:
    """Converts a PIL Image to a byte array (PNG format)."""
    img_byte_arr = io.BytesIO()
    image.save(img_byte_arr, format='PNG') # Using PNG for potentially better quality for OCR tasks
    img_byte_arr = img_byte_arr.getvalue()
    return img_byte_arr

def analyze_pdf_page_with_gemini(image_bytes: bytes) -> Dict[str, Any]:
    """
    Sends a single PDF page image (as bytes) to Gemini Pro Vision
    and requests structured field identification.
    """
    image_part = {"mime_type": "image/png", "data": image_bytes}

    # Construct the prompt for Gemini
    # This prompt is crucial and may need refinement based on observed results
    # and the specific structure desired for downstream processing.
    prompt = """
Analyze the provided image of a **BLANK** tax form page. Identify all elements intended for user input or selection, such as input fields, checkboxes, and radio buttons. For each identified element, extract its structural metadata.

Return the analysis as a JSON object. The JSON object MUST have a top-level key named 'fields'.
The value of 'fields' MUST be a list of objects, where each object represents a detected field or input element.

Each field object MUST contain the following keys:
- 'field_name': A concise, computer-friendly name suitable for use as a variable or key (e.g., "FirstName", "SSN", "FilingStatusSingle", "Line1aWages", "SchedC_Line12_OfficeExpense"). Use standard tax terminology and camelCase or snake_case consistently.
- 'field_type': The type of the input element (e.g., "text_input", "numeric_input", "date_input", "ssn_input", "checkbox", "radio_button", "signature_area"). Be specific where possible (e.g., distinguish numeric from general text).
- 'label_text': The exact text label visually associated with the field. If multiple labels seem relevant, choose the most direct and complete one. Capture the full label text.
- 'line_number': (Optional) The official form line number associated with the field, if present (e.g., "1a", "7", "Schedule C Line 12"). Return null if not applicable.
- 'section_name': (Optional) The logical section or grouping the field belongs to, based on visual headings or context (e.g., "Your first name and initial", "Filing Status", "Income", "Adjustments to Income", "Part II - Expenses"). Return null if not obvious.
- 'description': (Optional) A brief text description of the field's purpose or content, often found near the label or inferred from context (e.g., "Enter wages, salaries, tips, etc.", "Check if single", "Amount from Schedule 1, line 10"). Return null if not available.
- 'location_hint': (Optional but recommended) A brief textual description of where the field is located on the page (e.g., "Top left corner", "Below Filing Status checkboxes", "Line 2b, middle column").

Example of a text input field on Form 1040:
{
  "field_name": "FirstNameInitial",
  "field_type": "text_input",
  "label_text": "Your first name and initial",
  "line_number": null,
  "section_name": "Your first name and initial",
  "description": "Enter your first name and middle initial, if any.",
  "location_hint": "Top section, first main input area"
}

Example of a checkbox field on Form 1040:
{
  "field_name": "FilingStatusMarriedJointly",
  "field_type": "checkbox",
  "label_text": "Married filing jointly",
  "line_number": null,
  "section_name": "Filing Status",
  "description": "Check this box if your filing status is Married Filing Jointly.",
  "location_hint": "Filing Status section, second checkbox"
}

Example of a numeric input field on Form 1040:
{
  "field_name": "Line1aWages",
  "field_type": "numeric_input",
  "label_text": "Wages, salaries, tips, etc. Attach Form(s) W-2",
  "line_number": "1a",
  "section_name": "Income",
  "description": "Total amount of wages, salaries, tips from W-2 forms.",
  "location_hint": "Income section, line 1a"
}


Focus ONLY on elements intended for user input or selection. Do not include general instructional text blocks, headers, or footers unless they act as a direct label or provide essential context (like section name) for a specific field.
The response MUST contain ONLY the JSON object and nothing else (no introductory text, explanations, or markdown formatting like ```json).
"""

    try:
        # Generate content using the model
        # Consider adding safety_settings if needed
        response = model.generate_content([prompt, image_part], stream=False)
        response.resolve() # Ensure completion

        # Robust JSON parsing: Handle potential markdown code fences or surrounding text
        raw_text = response.text.strip()
        json_text = raw_text
        if raw_text.startswith("```json"):
            json_text = raw_text[7:]
        if raw_text.endswith("```"):
            json_text = json_text[:-3]
        json_text = json_text.strip() # Remove any leading/trailing whitespace

        if not json_text.startswith("{") or not json_text.endswith("}"):
             print(f"Warning: Gemini response does not appear to be a valid JSON object:\n"
                   f"{raw_text}")
             # Attempt to find JSON within the text if possible
             json_start = raw_text.find('{')
             json_end = raw_text.rfind('}') + 1
             if json_start != -1 and json_end > json_start:
                  json_text = raw_text[json_start:json_end]
                  print("Attempting to parse extracted JSON substring.")
             else:
                  return {"error": "Failed to find valid JSON in Gemini response.", "raw_response": raw_text}

        # Parse the cleaned JSON string
        parsed_json = json.loads(json_text)
        # Basic validation of structure
        if 'fields' not in parsed_json or not isinstance(parsed_json['fields'], list):
             print(f"Warning: Parsed JSON missing 'fields' list key:\n"
                   f"{parsed_json}")
             return {"error": "Parsed JSON missing 'fields' list.", "raw_response": raw_text, "parsed_json": parsed_json}

        return parsed_json

    except json.JSONDecodeError as json_err:
        print(f"Error decoding JSON from Gemini response: {json_err}")
        print(f"Raw response text:\n{raw_text}")
        return {"error": f"JSONDecodeError: {json_err}", "raw_response": raw_text}
    except Exception as e:
        # Catch other potential API errors (network issues, invalid API key, quota limits, safety blocks)
        print(f"Error during Gemini API call or processing: {e}")
        # Check for specific feedback if available
        try:
            if response and response.prompt_feedback:
                 print(f"Prompt Feedback: {response.prompt_feedback}")
            if response and response.candidates and response.candidates[0].finish_reason:
                 print(f"Candidate Finish Reason: {response.candidates[0].finish_reason}")
                 if response.candidates[0].finish_reason != "STOP":
                      print(f"Safety Ratings: {response.candidates[0].safety_ratings}")

        except Exception as feedback_err:
             print(f"Could not retrieve detailed feedback: {feedback_err}")

        return {"error": f"Gemini API call failed: {e}", "raw_response": raw_text if 'raw_text' in locals() else "N/A"}


def extract_fields_from_pdf_gemini(pdf_path: str) -> Dict[str, Any]:
    """
    Extracts form fields from a PDF using Gemini Pro Vision by analyzing each page.

    Returns a dictionary where keys are page numbers (1-indexed) and
    values are the structured field data (or error info) returned by Gemini for that page.
    """
    if not os.path.exists(pdf_path):
        return {"error": f"PDF file not found: {pdf_path}"}

    print(f"Starting Gemini field extraction for: {pdf_path}")
    try:
        images = pdf_to_images(pdf_path)
    except Exception as img_err:
        # Error during PDF conversion, return immediately
        return {"error": f"Failed to convert PDF to images: {img_err}"}

    all_pages_data = {}
    print(f"Found {len(images)} page(s) in the PDF.")

    for i, page_image in enumerate(images):
        page_num = i + 1
        print(f"Analyzing page {page_num}/{len(images)}...")
        try:
            image_bytes = image_to_byte_array(page_image)
            page_data = analyze_pdf_page_with_gemini(image_bytes)
            all_pages_data[f"page_{page_num}"] = page_data
            # Optional: Add a small delay between API calls to respect rate limits if necessary
            # time.sleep(1) # Delay for 1 second
        except Exception as page_err:
            print(f"Error processing page {page_num}: {page_err}")
            all_pages_data[f"page_{page_num}"] = {"error": f"Failed to process page: {page_err}"}
        finally:
            # Explicitly close the image object to free memory, especially important for large PDFs
            page_image.close()


    print("Gemini analysis complete.")
    return all_pages_data

# --- Example Usage ---
if __name__ == '__main__':
    # --- Configuration ---
    # 1. Create a file named .env in the root of this project (i.e., next to this script if run directly, or in the workspace root /Users/master/Downloads/Auto-Apply)
    # 2. Add your Google AI Studio API key to the .env file like this:
    #    GEMINI_API_KEY='YOUR_API_KEY_HERE'
    # 3. Ensure you have installed poppler (see pdf_to_images comments for instructions)
    # 4. Install required Python packages: pip install google-generativeai pdf2image Pillow python-dotenv
    # 5. CHANGE THE `example_pdf` path below to point to an actual PDF form.

    # Get input PDF path from command line argument
    if len(sys.argv) > 1:
        example_pdf = sys.argv[1] 
    else:
        print("Error: No input PDF path provided.")
        print("Usage: python pdf_extraction/gemini_extractor.py <path_to_pdf>")
        sys.exit(1) # Exit if no path is given
        
    # OLD Hardcoded path:
    # example_pdf = 'templates/f1040sc_blank.pdf' # <--- CHANGE THIS PATH
    output_dir = 'output' # Directory to save the JSON results

    # --- Execution ---
    if not os.path.exists(example_pdf):
        print(f"Error: Input PDF not found at '{example_pdf}'")
        # print("Please update the 'example_pdf' variable in the script with a valid path.")
        # print("Make sure the .env file with your GEMINI_API_KEY is in the project root.")
    else:
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

        print(f"Running PDF field extraction for: {example_pdf}")
        extracted_data = extract_fields_from_pdf_gemini(example_pdf)

        # Determine output filename (This part correctly uses the base filename)
        base_filename = os.path.splitext(os.path.basename(example_pdf))[0]
        output_filename = os.path.join(output_dir, f"{base_filename}_gemini_extracted_fields.json")

        print(f"Saving extracted data to: {output_filename}")
        try:
            with open(output_filename, 'w') as f:
                json.dump(extracted_data, f, indent=4) # Use indent=4 for better readability
            print("Successfully saved data.")
        except IOError as e:
            print(f"Error saving data to JSON file: {e}")
        except TypeError as e:
             print(f"Error: Data structure cannot be serialized to JSON: {e}")
             print("Printing raw data instead:")
             print(extracted_data)

        # Optionally print a summary
        print("Extraction Summary:")
        has_errors = False
        for page, data in extracted_data.items():
            if isinstance(data, dict) and 'error' in data:
                print(f"- {page}: Error - {data['error']}")
                has_errors = True
            elif isinstance(data, dict) and 'fields' in data:
                 field_count = len(data['fields'])
                 print(f"- {page}: Success - Found {field_count} fields.")
            else:
                 print(f"- {page}: Unknown status or invalid data structure.")
                 has_errors = True
        if not has_errors:
             print("All pages processed successfully (though individual field detection quality depends on Gemini).")