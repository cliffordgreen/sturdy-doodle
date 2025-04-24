"""
Task for ingesting and preprocessing input documents/transcripts.
Uses PyMuPDF (fitz) for PDF handling.
"""

import os
import shutil
from typing import Dict, Any, Tuple, List
from prefect import task
from pathlib import Path

# Import necessary libraries
from PIL import Image # For handling images
import cv2 # For image preprocessing
import numpy as np # For image conversion

try:
    import fitz # PyMuPDF
    PYMUPDF_AVAILABLE = True
except ImportError:
    print("Warning: PyMuPDF (fitz) library not found. PDF processing will fail.")
    PYMUPDF_AVAILABLE = False
    fitz = None

TEMP_PROCESSED_DIR = "temp_processed"
IMAGE_DPI = 300 # DPI for PDF to image conversion
TEXT_LENGTH_THRESHOLD_FOR_IMAGE_CONVERSION = 150 # Chars per page heuristic

def _pixmap_to_pil(pixmap: fitz.Pixmap) -> Image.Image:
    """Converts a PyMuPDF Pixmap to a PIL Image."""
    if pixmap.alpha:
        img = Image.frombytes("RGBA", [pixmap.width, pixmap.height], pixmap.samples)
    else:
        img = Image.frombytes("RGB", [pixmap.width, pixmap.height], pixmap.samples)
    return img

def _save_pil_images(images: List[Image.Image], base_output_path: str):
    """Saves a list of PIL Images, returning their paths."""
    image_paths = []
    for i, img in enumerate(images):
        output_path = f"{base_output_path}_page_{i+1}.png" # Save as PNG
        try:
            img.save(output_path, "PNG")
            image_paths.append(output_path)
        except Exception as e:
            print(f"Error saving image {output_path}: {e}")
    return image_paths

def _load_document_pymupdf(file_path: str) -> Tuple[Any, str, List[str]]:
    """Loads document using PyMuPDF. Extracts text or converts to images."""
    file_ext = Path(file_path).suffix.lower()
    print(f"Loading document {file_path} with PyMuPDF...")
    processed_paths = [] # Store paths to processed files (text or images)
    os.makedirs(TEMP_PROCESSED_DIR, exist_ok=True)
    base_output_name = f"processed_{Path(file_path).stem}"
    text_output_path = os.path.join(TEMP_PROCESSED_DIR, f"{base_output_name}.txt")
    base_img_path = os.path.join(TEMP_PROCESSED_DIR, base_output_name)

    if file_ext == ".pdf":
        if not PYMUPDF_AVAILABLE:
             print("PyMuPDF not available, cannot process PDF.")
             return None, "error", []
        
        doc = None
        try:
            doc = fitz.open(file_path)
            num_pages = len(doc)
            if num_pages == 0:
                print("Warning: PDF has no pages.")
                return None, "empty", []

            # 1. Try extracting text
            all_text = []
            for page_num in range(num_pages):
                page = doc.load_page(page_num)
                # Options: "text", "html", "json", "xml", "xhtml" - "text" is simplest
                text = page.get_text("text") 
                all_text.append(text or "")
            
            full_text = "\n".join(all_text).strip()
            
            # Heuristic: Check if enough text was extracted
            if len(full_text) > TEXT_LENGTH_THRESHOLD_FOR_IMAGE_CONVERSION * num_pages:
                print("Successfully extracted text from PDF using PyMuPDF.")
                with open(text_output_path, "w", encoding='utf-8') as f:
                    f.write(full_text)
                processed_paths.append(text_output_path)
                return full_text, "text", processed_paths
            else:
                print(f"Low text content extracted ({len(full_text)} chars). Attempting image conversion.")

            # 2. If not enough text, convert to images
            print(f"Converting PDF to images (DPI={IMAGE_DPI}) using PyMuPDF...")
            images_pil = []
            for page_num in range(num_pages):
                page = doc.load_page(page_num)
                pix = page.get_pixmap(dpi=IMAGE_DPI)
                img_pil = _pixmap_to_pil(pix)
                images_pil.append(img_pil)

            if images_pil:
                image_paths = _save_pil_images(images_pil, base_img_path)
                if image_paths:
                    print(f"Successfully converted PDF to {len(image_paths)} image(s) using PyMuPDF.")
                    processed_paths.extend(image_paths)
                    # Return the PIL images and their paths
                    return images_pil, "image_list", processed_paths
                else:
                    print("Error saving converted images.")
                    return None, "error", []
            else:
                print("PyMuPDF conversion returned no images.")
                return None, "error", []

        except Exception as e:
            print(f"Error processing PDF {file_path} with PyMuPDF: {e}")
            return None, "error", []
        finally:
            if doc: doc.close()

    elif file_ext in [".jpg", ".jpeg", ".png", ".tiff", ".tif", ".bmp"]:
        # Handle standard image types
        try:
            img = Image.open(file_path)
            img_output_path = f"{base_img_path}.png" # Save as PNG
            img.save(img_output_path, "PNG")
            processed_paths.append(img_output_path)
            print("Loaded and saved image file.")
            return [img], "image_list", processed_paths
        except Exception as e:
             print(f"Error loading image {file_path}: {e}")
             return None, "error", []

    elif file_ext == ".txt":
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            with open(text_output_path, "w", encoding='utf-8') as f:
                f.write(content)
            processed_paths.append(text_output_path)
            print("Loaded and saved text file.")
            return content, "text", processed_paths
        except Exception as e:
            print(f"Error loading text file {file_path}: {e}")
            return None, "error", []
    else:
        print(f"Warning: Unsupported file type {file_ext} for {file_path}")
        raise ValueError(f"Unsupported file type: {file_ext}")

def _preprocess_data(data: Any, data_type: str, processed_paths: List[str]) -> Tuple[Any, List[str]]:
    """Applies preprocessing. Image preprocessing uses OpenCV on file paths."""
    print(f"Preprocessing data of type: {data_type}")
    output_paths = processed_paths # Start with existing paths

    if data_type == "image_list":
        print("Applying image preprocessing (basic thresholding)... ")
        # We operate on the saved image files from the previous step
        preprocessed_paths = [] 
        for i, img_path in enumerate(processed_paths):
            try:
                cv_img = cv2.imread(img_path, cv2.IMREAD_GRAYSCALE) 
                if cv_img is None:
                    print(f"Warning: Could not read image {img_path} with OpenCV for preprocessing.")
                    preprocessed_paths.append(img_path) # Keep original if read fails
                    continue
                
                processed_cv_img = cv2.adaptiveThreshold(
                    cv_img, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, 
                    cv2.THRESH_BINARY, 11, 5 # Adjust block size, C value!
                )
                
                processed_img_path = f"{os.path.splitext(img_path)[0]}_preprocessed.png"
                success = cv2.imwrite(processed_img_path, processed_cv_img)
                if success:
                    preprocessed_paths.append(processed_img_path)
                    # One could delete the non-preprocessed intermediate file here if desired
                    # if processed_img_path != img_path: os.remove(img_path)
                else:
                    print(f"Warning: Failed to write preprocessed image {processed_img_path}")
                    preprocessed_paths.append(img_path) # Keep original if write fails

            except Exception as e:
                print(f"Error during image preprocessing for {img_path}: {e}")
                preprocessed_paths.append(img_path) # Keep original path on error
        
        # Return the original data (PIL images list) and the paths to the *preprocessed* files
        return data, preprocessed_paths

    elif data_type == "text":
        print("Applying text normalization (stripping whitespace)...")
        if isinstance(data, str):
            normalized_text = data.strip()
            # Overwrite the processed text file
            if output_paths:
                try:
                    with open(output_paths[0], "w", encoding='utf-8') as f:
                        f.write(normalized_text)
                except Exception as e:
                    print(f"Error overwriting processed text file {output_paths[0]}: {e}")
            return normalized_text, output_paths
        else:
            print("Warning: Expected string data for text normalization.")
            return data, output_paths
    else:
        print(f"No preprocessing defined for type {data_type}")
        return data, output_paths

@task(retries=1, retry_delay_seconds=5)
def ingest_and_preprocess(file_path: str) -> Dict[str, Any]:
    """Loads, preprocesses using PyMuPDF and other libraries."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Input file not found: {file_path}")

    print(f"Starting ingestion and preprocessing for: {file_path} using PyMuPDF backend")
    processed_info = {
        "original_path": file_path,
        "processed_paths": [],
        "data_type": "error",
        "status": "FAILED"
    }

    try:
        # Load document using PyMuPDF based function
        data, data_type, initial_paths = _load_document_pymupdf(file_path)
        processed_info["data_type"] = data_type
        processed_info["processed_paths"] = initial_paths

        if data_type == "error" or data is None:
            print(f"Failed to load document {file_path}. Aborting task.")
            return processed_info

        # Preprocess data (operates on files in processed_paths for images)
        _processed_data, final_paths = _preprocess_data(data, data_type, initial_paths)
        processed_info["processed_paths"] = final_paths

        if final_paths:
            processed_info["status"] = "SUCCESS"
            print(f"Ingestion/Preprocessing complete for: {file_path}. Output info: {processed_info}")
        else:
            print(f"Ingestion/Preprocessing failed for {file_path}, no output files generated.")
            # Status remains FAILED

        return processed_info

    except Exception as e:
        print(f"Error during ingestion/preprocessing task for {file_path}: {e}", exc_info=True)
        processed_info["error_message"] = str(e)
        return processed_info 