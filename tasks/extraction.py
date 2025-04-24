"""
Task for Text Extraction (OCR) and Layout Analysis.
Uses PaddleOCR as an example implementation.
"""

from typing import Dict, Any, List
from prefect import task
import os
from pathlib import Path
import numpy as np # For potential image manipulation if needed

# Import PaddleOCR
try:
    from paddleocr import PaddleOCR
    # Initialize PaddleOCR. Specify languages, use GPU if available (use_gpu=True/False)
    # Download models by setting det=True, rec=True, cls=True on first run or if needed.
    # Layout analysis is implicitly handled by default in recent versions, or use specific layout models.
    ocr_engine = PaddleOCR(use_angle_cls=True, lang='en', use_gpu=False, show_log=False)
    PADDLE_AVAILABLE = True
    print("PaddleOCR engine initialized.")
except ImportError:
    print("Warning: paddleocr library not found. OCR/Layout extraction will use basic simulation.")
    ocr_engine = None
    PADDLE_AVAILABLE = False
except Exception as e:
    print(f"Warning: Failed to initialize PaddleOCR engine: {e}. Using simulation.")
    ocr_engine = None
    PADDLE_AVAILABLE = False

def _run_ocr_layout_analysis_paddle(processed_paths: List[str], data_type: str) -> Dict[str, Any]:
    """Performs OCR and Layout Analysis using PaddleOCR."""
    print(f"Running PaddleOCR on {len(processed_paths)} file(s) (type: {data_type})")
    all_pages_results = []

    if data_type == "text":
        # If data is text, just structure it simply, no OCR needed
        if not processed_paths or not os.path.exists(processed_paths[0]):
             return {"pages": [], "error": "Processed text file not found"}
        try:
            with open(processed_paths[0], 'r', encoding='utf-8') as f:
                content = f.read()
            lines = content.split('\n')
            page_elements = []
            for i, line in enumerate(lines):
                 if line.strip():
                    page_elements.append({
                        "text": line.strip(),
                        "bbox": [10, 10 + i*12, 600, 20 + i*12], # Assign arbitrary bbox
                        "confidence": 1.0, # Confidence for text file is high
                        "layout_type": "line" # Simple layout type
                    })
            all_pages_results.append({"page_num": 1, "elements": page_elements})
            print("Structured text from input file.")
        except Exception as e:
             print(f"Error reading processed text file {processed_paths[0]}: {e}")
             return {"pages": [], "error": f"Error reading text file: {e}"}

    elif data_type == "image_list":
        # Process list of image file paths
        for i, img_path in enumerate(processed_paths):
            print(f"Processing image: {img_path}")
            if not os.path.exists(img_path):
                print(f"Warning: Image file not found: {img_path}. Skipping.")
                continue
            try:
                # Run OCR using PaddleOCR
                # result is a list of lists, one per page (usually 1 for single image input)
                # Each inner list contains [bbox, (text, confidence)]
                result = ocr_engine.ocr(img_path, cls=True) # cls=True for angle correction
                
                page_elements = []
                if result and result[0]: # Check if result is not None and contains data for the first page
                    for item in result[0]:
                        # item = [bbox, (text, confidence)]
                        # bbox = [[x1,y1], [x2,y2], [x3,y3], [x4,y4]]
                        box = item[0]
                        text, confidence = item[1]
                        
                        # Convert bbox to a more standard [xmin, ymin, xmax, ymax] format (approximate)
                        x_coords = [p[0] for p in box]
                        y_coords = [p[1] for p in box]
                        bbox_std = [min(x_coords), min(y_coords), max(x_coords), max(y_coords)]
                        
                        page_elements.append({
                            "text": text,
                            "bbox": bbox_std,
                            "confidence": round(confidence, 4),
                            "layout_type": "ocr_line" # Basic type, layout analysis model would give more detail
                        })
                    all_pages_results.append({"page_num": i + 1, "elements": page_elements})
                    print(f"PaddleOCR successful for page {i+1}. Found {len(page_elements)} elements.")
                else:
                    print(f"PaddleOCR returned no results for page {i+1}.")
                    all_pages_results.append({"page_num": i + 1, "elements": []})

            except Exception as e:
                print(f"Error during PaddleOCR processing for {img_path}: {e}")
                # Add empty page result on error? Or append an error marker?
                all_pages_results.append({"page_num": i + 1, "elements": [], "error": str(e)})
    else:
        return {"pages": [], "error": f"Unsupported data_type for OCR: {data_type}"}

    print(f"OCR/Layout analysis complete. Processed {len(all_pages_results)} page(s).")
    return {"pages": all_pages_results}

def _run_ocr_layout_analysis_simulation(processed_paths: List[str], data_type: str) -> Dict[str, Any]:
    """Placeholder simulation if PaddleOCR is not available."""
    print("Running OCR/Layout Analysis Simulation...")
    simulated_pages = []
    if not processed_paths:
        return {"pages": [], "error": "No processed files provided"}

    file_path_to_process = processed_paths[0] # Just use the first file for simulation
    if not os.path.exists(file_path_to_process):
        return {"pages": [], "error": f"Processed file not found: {file_path_to_process}"}

    try:
        if data_type == "text" or file_path_to_process.endswith('.txt'):
            # ... (simulation logic as before) ...
             with open(file_path_to_process, 'r') as f: content = f.read()
             lines = content.split('\n'); page_content = [] ; para_text = ""
             for i, line in enumerate(lines):
                 if line.strip(): para_text += line + " "
                 elif para_text: page_content.append({"text": para_text.strip(), "bbox": [50, 50 + i*15, 550, 65 + i*15], "layout_type": "paragraph"}); para_text = ""
             if para_text: page_content.append({"text": para_text.strip(), "bbox": [50, 50 + len(lines)*15, 550, 65 + len(lines)*15], "layout_type": "paragraph"})
             simulated_pages.append({"page_num": 1, "elements": page_content})
        elif data_type == "image_list":
            # ... (simulation logic as before) ...
             simulated_pages.append({"page_num": 1,"elements": [{"text": "Simulated Header", "bbox": [50, 20, 550, 40], "layout_type": "header"},{"text": "Simulated text block 1 from image.", "bbox": [50, 60, 550, 100], "layout_type": "paragraph"},{"text": "Simulated text block 2.", "bbox": [50, 110, 550, 150], "layout_type": "paragraph"}]})
    except Exception as e:
        return {"pages": [], "error": str(e)}
    return {"pages": simulated_pages}

@task
def extract_text_layout(processed_info: Dict[str, Any]) -> Dict[str, Any]:
    """Prefect task wrapping OCR and Layout Analysis."""
    processed_paths = processed_info.get('processed_paths')
    data_type = processed_info.get('data_type')
    original_path = processed_info.get('original_path')

    if processed_info.get("status") != "SUCCESS" or not processed_paths or not data_type:
        error_msg = processed_info.get("error_message", "Ingestion/Preprocessing failed or produced no output.")
        print(f"Skipping extraction due to previous step failure: {error_msg}")
        return {"pages": [], "error": error_msg, "original_path": original_path}

    print(f"Starting text and layout extraction from: {processed_paths}")

    if PADDLE_AVAILABLE and ocr_engine:
        structured_layout_data = _run_ocr_layout_analysis_paddle(processed_paths, data_type)
    else:
        print("PaddleOCR not available, falling back to simulation.")
        structured_layout_data = _run_ocr_layout_analysis_simulation(processed_paths, data_type)

    print(f"Text/Layout extraction finished for: {original_path}")

    # Add original path for reference
    structured_layout_data['original_path'] = original_path

    return structured_layout_data 