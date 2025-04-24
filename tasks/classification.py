"""
Task for classifying document types using Gemini.
"""
import os
from typing import List, Tuple, Dict, Any
from pathlib import Path
from prefect import task, get_run_logger

# Assuming Gemini setup and image conversion logic will be available
# This might require refactoring common utilities later.
# For now, we assume necessary imports and setup are similar to source_document_extractor.
try:
    import google.generativeai as genai
    from google.api_core.exceptions import GoogleAPIError
    from PIL import Image
    import fitz # PyMuPDF
    # TODO: Add configuration for API key (e.g., from environment variable)
    # genai.configure(api_key=os.environ["GEMINI_API_KEY"]) 
    GEMINI_AVAILABLE = True 
except ImportError:
    print("Warning: Gemini or dependent libraries (PIL, PyMuPDF) not found. Document classification cannot be performed.")
    GEMINI_AVAILABLE = False
    genai = None
    Image = None
    fitz = None
    GoogleAPIError = None

# --- Helper for PDF to Image (Simplified: First Page Only) ---
def _convert_pdf_to_image_first_page(pdf_path: str) -> Image.Image | None:
    """Converts the first page of a PDF to a PIL Image object."""
    if not fitz or not Image:
        return None
    doc = None # Ensure doc is defined in outer scope for finally block
    try:
        doc = fitz.open(pdf_path)
        if not doc or doc.page_count == 0:
            print(f"Warning: Could not open or empty PDF: {pdf_path}")
            return None
            
        page = doc.load_page(0)  # Load the first page (index 0)
        pix = page.get_pixmap(dpi=200) # Render at 200 DPI
        
        # Use the built-in method for conversion
        img = pix.pil_image()
        
        # Ensure it's RGB if needed by Gemini (pil_image might return other modes like L or LA)
        if img.mode != "RGB":
             print(f"DEBUG: Converting image mode from {img.mode} to RGB for {pdf_path}")
             img = img.convert("RGB")
            
        return img
        
    except Exception as e:
        print(f"Error converting first page of PDF {pdf_path} to image: {e}")
        return None
    finally:
         # Ensure the document is closed
         if doc:
             try:
                 doc.close()
             except Exception as close_err:
                 print(f"Error closing PDF {pdf_path} during image conversion cleanup: {close_err}")

# --- Gemini Classification Call ---
def _call_gemini_for_classification(image: Image.Image, filename: str) -> str:
    """Calls Gemini API to classify the document type."""
    if not genai or not GEMINI_AVAILABLE:
        return "Other" # Default if Gemini unavailable

    logger = get_run_logger()
    
    # Configure the model (use a vision-capable model)
    # Make sure 'gemini-1.5-flash' or similar vision model is available/selected
    model = genai.GenerativeModel('gemini-1.5-flash') 
    
    # Define a list of expected, standardized document types
    # Keep this list relatively concise
    standard_types = [
        "W-2", 
        "1099-NEC", 
        "1099-INT", 
        "1099-DIV", 
        "1099-MISC",
        "Profit and Loss Statement", 
        "Balance Sheet",
        "Cash Flow Statement",
        "Invoice", 
        "Receipt", 
        "Bank Statement", 
        "Insurance Policy",
        "Other" # Catch-all
    ]
    
    # Create the prompt
    prompt = f"""
    Analyze the provided image, which is the first page of a document named '{filename}'. 
    Based *only* on the visual content and layout of the image and the filename, classify the document type.
    Return *only* one classification from the following list:
    {', '.join(standard_types)}
    
    Focus on identifying standard financial or tax forms. If it does not clearly match any category other than "Other", return "Other". 
    Do not add any explanations or extra text.
    """

    max_retries = 3
    for attempt in range(max_retries):
        try:
            logger.info(f"Calling Gemini for classification (Attempt {attempt + 1}/{max_retries})...")
            # Generate content using the image and prompt
            response = model.generate_content([prompt, image])
            
            # Basic response parsing and validation
            if response and response.text:
                 result_type = response.text.strip()
                 # Validate against our standard list
                 if result_type in standard_types:
                      logger.info(f"Gemini classified as: {result_type}")
                      return result_type
                 else:
                      logger.warning(f"Gemini classification '{result_type}' not in standard list. Defaulting to 'Other'.")
                      return "Other"
            else:
                logger.warning("Gemini classification response was empty.")
                return "Other" # Default if response is empty

        except GoogleAPIError as e:
             logger.error(f"Gemini API error during classification (Attempt {attempt + 1}): {e}")
             if attempt == max_retries - 1:
                 return "Other" # Default after final retry
             # Optional: time.sleep(1) before retry
        except Exception as e:
            logger.error(f"Non-API error during Gemini classification (Attempt {attempt + 1}): {e}")
            # Decide if retry makes sense for non-API errors? For now, break and default.
            return "Other" # Default on unexpected errors
            
    return "Other" # Should not be reached if retries are handled, but as a failsafe


# --- Prefect Task ---
@task
def classify_document(doc_path: str, doc_filename: str) -> str:
    """
    Prefect task to classify a document's type using its path and filename.
    """
    logger = get_run_logger()
    logger.info(f"Starting classification for: {doc_filename}")
    
    if not GEMINI_AVAILABLE or not fitz or not Image:
         logger.error("Required libraries (Gemini, PyMuPDF, PIL) not available. Cannot classify.")
         return "Other"

    # 1. Convert first page to image
    image = _convert_pdf_to_image_first_page(doc_path)
    
    if image is None:
        logger.error(f"Failed to get image for classification: {doc_filename}")
        return "Other" # Default if image conversion fails

    # 2. Call Gemini for classification
    doc_type = _call_gemini_for_classification(image, doc_filename)
    
    logger.info(f"Classification complete for {doc_filename}: {doc_type}")
    return doc_type 