# data_extraction/source_document_extractor.py
import os
import google.generativeai as genai
from pdf2image import convert_from_path
from PIL import Image
import json
import io
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional
import time

# --- Setup (Similar to blank form extractor) ---
load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

if not GEMINI_API_KEY:
    raise ValueError("GEMINI_API_KEY not found in environment variables. Please set it in a .env file.")

genai.configure(api_key=GEMINI_API_KEY)
# Adjust model name if needed, e.g., 'gemini-1.5-pro-latest'
# Using gemini-pro-vision as it's required for image input
model = genai.GenerativeModel('gemini-2.5-flash-preview-04-17')

# --- PDF/Image Helpers (Could be moved to utils) ---

def pdf_to_images(pdf_path: str) -> List[Image.Image]:
    """Converts a PDF file into a list of PIL Image objects."""
    try:
        images = convert_from_path(pdf_path)
        return images
    except Exception as e:
        print(f"Error converting PDF {pdf_path} to images: {e}")
        print("Ensure poppler is installed and in your system's PATH.")
        raise

def image_to_byte_array(image: Image.Image) -> bytes:
    """Converts a PIL Image to a byte array (PNG format)."""
    img_byte_arr = io.BytesIO()
    image.save(img_byte_arr, format='PNG')
    img_byte_arr = img_byte_arr.getvalue()
    return img_byte_arr

# --- Core Gemini Data Extraction Function ---

# Define prompts for different document types
PROMPT_GENERIC = """
Analyze the provided image, which could be a page from a tax form (like W-2, 1099-INT, 1099-DIV, 1099-NEC), a receipt, an invoice, or a financial statement (like a P&L).
Identify and extract key information relevant for tax preparation.

Return the analysis STRICTLY as a single JSON object.
This JSON object should map standardized keys (listed below) to the extracted values found in the document.
Extract the value exactly as it appears on the document, including currency symbols, formatting, and full text where appropriate.
If a specific piece of information corresponding to a key is not found on this page, OMIT the key entirely from the JSON output. Do not include keys with null or empty values.

Attempt to determine the DocumentType first from this list: "W-2", "1099-NEC", "1099-INT", "1099-DIV", "1099-MISC", "Profit and Loss Statement", "Balance Sheet", "Cash Flow Statement", "Invoice", "Receipt", "Bank Statement", "Insurance Policy", "Other".

Possible Standardized Keys:
- DocumentType, TaxYear
- EmployerName, EmployerAddress, EmployerEIN
- EmployeeName, EmployeeAddress, EmployeeSSN
- WagesTipsOtherComp, FederalIncomeTaxWithheld, SocialSecurityWages, SocialSecurityTaxWithheld, MedicareWagesAndTips, MedicareTaxWithheld
- StateName, StateEmployerID, StateWagesTipsEtc, StateIncomeTax
- LocalWagesTipsEtc, LocalIncomeTax, LocalityName
- PayerName, PayerTIN, RecipientName, RecipientTIN
- InterestIncome, EarlyWithdrawalPenalty, InterestOnUSTreasuryObligations
- TotalOrdinaryDividends, QualifiedDividends, TotalCapitalGainDistrib, Section1202Gain, NondividendDistributions, ForeignTaxPaid
- NonemployeeCompensation
- VendorName, TransactionDate, TotalAmount, TaxAmount, ExpenseCategory, Description
- ReportingPeriodStartDate, ReportingPeriodEndDate, TotalRevenue, CostOfGoodsSold, GrossProfit, TotalOperatingExpenses, NetIncomeLoss
- AccountNumber, PolicyNumber, PolicyPeriod, NamedInsured(s)
- Other relevant identifiers or amounts.

The response MUST contain ONLY the JSON object and nothing else.
"""

PROMPT_W2 = """
Analyze the provided image, which is confirmed to be a page from a W-2 form.
Identify and extract information relevant to Form W-2.

Return the analysis STRICTLY as a single JSON object.
This JSON object should map standardized keys (listed below) to the extracted values found in the document.
Extract the value exactly as it appears on the document, including formatting.
If a specific piece of information corresponding to a key is not found on this page, OMIT the key entirely from the JSON output.

Standardized Keys for W-2:
- DocumentType: (Should be "W-2")
- TaxYear
- EmployerName
- EmployerAddress
- EmployerEIN
- EmployeeName
- EmployeeAddress
- EmployeeSSN
- WagesTipsOtherComp: (Box 1)
- FederalIncomeTaxWithheld: (Box 2)
- SocialSecurityWages: (Box 3)
- SocialSecurityTaxWithheld: (Box 4)
- MedicareWagesAndTips: (Box 5)
- MedicareTaxWithheld: (Box 6)
- SocialSecurityTips: (Box 7)
- AllocatedTips: (Box 8)
- DependentCareBenefits: (Box 10)
- NonqualifiedPlans: (Box 11)
- StatutoryEmployee: (Box 13 Checkbox - return true/false or value if indicated)
- RetirementPlan: (Box 13 Checkbox - return true/false or value if indicated)
- ThirdPartySickPay: (Box 13 Checkbox - return true/false or value if indicated)
- OtherBox12CodesAndAmounts: (Box 12 a-d - Extract Code and Amount, e.g., {"D": "1500.00", "DD": "5000.00"})
- OtherBox14Items: (Box 14 - Extract Description and Amount, e.g., {"CASDI": "123.45"})
- StateName: (Box 15 State)
- StateEmployerID: (Box 15 Employer's state ID number)
- StateWagesTipsEtc: (Box 16)
- StateIncomeTax: (Box 17)
- LocalWagesTipsEtc: (Box 18)
- LocalIncomeTax: (Box 19)
- LocalityName: (Box 20)

The response MUST contain ONLY the JSON object and nothing else.
"""

PROMPT_PNL = """
Analyze the provided image, which is confirmed to be a page from a Profit and Loss Statement (P&L) or Income Statement.
Identify and extract key financial figures and information.

Return the analysis STRICTLY as a single JSON object.
This JSON object should map standardized keys (listed below) to the extracted values found in the document.
Extract the value exactly as it appears on the document, including currency symbols or formatting (like parentheses for negatives).
If a specific piece of information is not found, OMIT the key entirely from the JSON output.

Standardized Keys for P&L:
- DocumentType: (Should be "Profit and Loss Statement")
- BusinessName
- PropertyAddress: (Full street address, city, state, zip if associated with a specific property)
- ReportingPeriodStartDate
- ReportingPeriodEndDate
- TotalRevenue (or Sales, Income)
- CostOfGoodsSold (or Cost of Sales)
- GrossProfit
- AdvertisingExpense
- SalariesWagesExpense
- RentExpense
- UtilitiesExpense
- InsuranceExpense
- RepairsMaintenanceExpense
- OfficeSuppliesExpense
- LegalProfessionalExpense
- DepreciationExpense
- InterestExpense
- TaxesLicensesExpense
- TravelExpense
- MealsEntertainmentExpense
- BankChargesFeesExpense
- TotalOperatingExpenses
- OperatingIncome (or EBIT - Earnings Before Interest and Taxes)
- InterestIncome
- OtherIncome
- TotalOtherIncome
- InterestExpense (if separate from operating section)
- OtherExpenses (non-operating)
- TotalOtherExpenses
- IncomeBeforeTax (or Pretax Income)
- IncomeTaxExpense
- NetIncomeLoss

The response MUST contain ONLY the JSON object and nothing else.
"""

PROMPT_CASHFLOW = """
Analyze the provided image, which is confirmed to be a page from a Cash Flow Statement.
Identify and extract key financial figures and information, especially property address if present.

Return the analysis STRICTLY as a single JSON object.
This JSON object should map standardized keys (listed below) to the extracted values found in the document.
Extract the value exactly as it appears on the document, including currency symbols or formatting.
If a specific piece of information is not found, OMIT the key entirely from the JSON output.

Standardized Keys for Cash Flow:
- DocumentType: (Should be "Cash Flow Statement")
- BusinessName
- PropertyAddress: (Full street address, city, state, zip if associated with a specific property)
- ReportingPeriodStartDate
- ReportingPeriodEndDate
- TotalRevenue (or RentalIncome, OtherIncome)
- InsuranceExpense
- RepairsMaintenanceExpense
- TaxesExpense
- UtilitiesExpense
- ManagementFeeExpense
- MortgageInterestExpense
- OtherExpenseCategory: (e.g., HOA Dues, Supplies)
- OtherExpenseAmount
- TotalOperatingExpenses
- NetIncomeLoss (or NetCashFlow)

The response MUST contain ONLY the JSON object and nothing else.
"""

PROMPT_INVOICE = """
Analyze the provided image, which is confirmed to be a page from an Invoice.
Identify and extract key information.

Return the analysis STRICTLY as a single JSON object.
This JSON object should map standardized keys (listed below) to the extracted values found in the document.
Extract the value exactly as it appears.
If a specific piece of information is not found, OMIT the key.

Standardized Keys for Invoice:
- DocumentType: (Should be "Invoice")
- InvoiceNumber
- InvoiceDate
- DueDate
- VendorName (Seller Name)
- VendorAddress
- VendorPhoneNumber
- CustomerName (Buyer Name)
- CustomerAddress
- Description: (Brief description of overall service/product or line items)
- SubtotalAmount
- DiscountAmount
- TaxAmount (or VATAmount)
- ShippingHandlingAmount
- TotalAmount
- AmountPaid
- BalanceDue

The response MUST contain ONLY the JSON object and nothing else.
"""

# TODO: Add prompts for other specific types like 1099s, Receipts, Insurance etc.

PROMPT_1099_NEC = """
Analyze the provided image, which is confirmed to be a page from a 1099-NEC form.
Identify and extract information relevant to Form 1099-NEC (Nonemployee Compensation).

Return the analysis STRICTLY as a single JSON object.
This JSON object should map standardized keys (listed below) to the extracted values found in the document.
Extract the value exactly as it appears on the document. Omit keys if the information is not found.

Standardized Keys for 1099-NEC:
- DocumentType: (Should be "1099-NEC")
- TaxYear
- PayerName
- PayerAddress (Combine street, city, state, zip)
- PayerTIN
- RecipientName
- RecipientAddress (Combine street, city, state, zip)
- RecipientTIN
- NonemployeeCompensation: (Box 1)
- DirectSalesIndicator: (Box 2 Checkbox - return true/false or value if indicated)
- FederalIncomeTaxWithheld_1099NEC: (Box 4)
- StateTaxWithheld_1099NEC: (Box 5 - May have multiple state entries, extract as list if possible e.g., [{"StateTaxWithheld": "100.00", "StatePayerStateNo": "12345", "StateIncome": "5000.00"}])
- StatePayerStateNo_1099NEC: (Box 6)
- StateIncome_1099NEC: (Box 7)
- AccountNumber

The response MUST contain ONLY the JSON object and nothing else.
"""

PROMPT_1099_INT = """
Analyze the provided image, which is confirmed to be a page from a 1099-INT form.
Identify and extract information relevant to Form 1099-INT (Interest Income).

Return the analysis STRICTLY as a single JSON object.
This JSON object should map standardized keys (listed below) to the extracted values found in the document.
Extract the value exactly as it appears. Omit keys if the information is not found.

Standardized Keys for 1099-INT:
- DocumentType: (Should be "1099-INT")
- TaxYear
- PayerName
- PayerAddress (Combine street, city, state, zip)
- PayerTIN
- RecipientName
- RecipientAddress (Combine street, city, state, zip)
- RecipientTIN
- InterestIncome: (Box 1)
- EarlyWithdrawalPenalty: (Box 2)
- InterestOnUSTreasuryObligations: (Box 3)
- FederalIncomeTaxWithheld_1099INT: (Box 4)
- InvestmentExpenses: (Box 5)
- ForeignTaxPaid: (Box 6)
- ForeignCountryOrUSPossession: (Box 7)
- TaxExemptInterest: (Box 8)
- SpecifiedPrivateActivityBondInterest: (Box 9)
- MarketDiscount: (Box 10)
- BondPremium: (Box 11)
- BondPremiumUSTreasuryObligations: (Box 13)
- AccountNumber

The response MUST contain ONLY the JSON object and nothing else.
"""

PROMPT_1099_DIV = """
Analyze the provided image, which is confirmed to be a page from a 1099-DIV form.
Identify and extract information relevant to Form 1099-DIV (Dividends and Distributions).

Return the analysis STRICTLY as a single JSON object.
This JSON object should map standardized keys (listed below) to the extracted values found in the document.
Extract the value exactly as it appears. Omit keys if the information is not found.

Standardized Keys for 1099-DIV:
- DocumentType: (Should be "1099-DIV")
- TaxYear
- PayerName
- PayerAddress (Combine street, city, state, zip)
- PayerTIN
- RecipientName
- RecipientAddress (Combine street, city, state, zip)
- RecipientTIN
- TotalOrdinaryDividends: (Box 1a)
- QualifiedDividends: (Box 1b)
- TotalCapitalGainDistrib: (Box 2a)
- UnrecapSec1250Gain: (Box 2b)
- Section1202Gain: (Box 2c)
- CollectiblesGain28Percent: (Box 2d)
- NondividendDistributions: (Box 3)
- FederalIncomeTaxWithheld_1099DIV: (Box 4)
- InvestmentExpenses_1099DIV: (Box 5)
- Section199ADividends: (Box 6)
- ForeignTaxPaid: (Box 7)
- ForeignCountryOrUSPossession: (Box 8)
- CashLiquidationDistrib: (Box 10)
- NoncashLiquidationDistrib: (Box 11)
- ExemptInterestDividends: (Box 12)
- SpecifiedPrivateActivityBondInterestDividends: (Box 13)
- StateTaxWithheld_1099DIV: (Box 15 - May have multiple state entries)
- StateIdentificationNo_1099DIV: (Box 16)
- AccountNumber

The response MUST contain ONLY the JSON object and nothing else.
"""

PROMPT_1099_MISC = """
Analyze the provided image, which is confirmed to be a page from a 1099-MISC form.
Identify and extract information relevant to Form 1099-MISC (Miscellaneous Income).

Return the analysis STRICTLY as a single JSON object.
This JSON object should map standardized keys (listed below) to the extracted values found in the document.
Extract the value exactly as it appears. Omit keys if the information is not found.

Standardized Keys for 1099-MISC:
- DocumentType: (Should be "1099-MISC")
- TaxYear
- PayerName
- PayerAddress (Combine street, city, state, zip)
- PayerTIN
- RecipientName
- RecipientAddress (Combine street, city, state, zip)
- RecipientTIN
- Rents: (Box 1)
- Royalties: (Box 2)
- OtherIncome_1099MISC: (Box 3)
- FederalIncomeTaxWithheld_1099MISC: (Box 4)
- FishingBoatProceeds: (Box 5)
- MedicalHealthcarePayments: (Box 6)
- DirectSalesIndicator_1099MISC: (Box 7 Checkbox - $5000 or more of sales - return true/false)
- SubstitutePayments: (Box 8)
- CropInsuranceProceeds: (Box 9)
- GrossProceedsAttorney: (Box 10)
- FishPurchasedResale: (Box 11)
- Section409ADeferrals: (Box 12)
- ExcessGoldenParachute: (Box 14)
- NonqualifiedDeferredCompensation: (Box 15)
- StateTaxWithheld_1099MISC: (Box 16 - May have multiple state entries)
- StatePayerStateNo_1099MISC: (Box 17)
- StateIncome_1099MISC: (Box 18)
- AccountNumber

The response MUST contain ONLY the JSON object and nothing else.
"""

def extract_data_from_source_document_page(image_bytes: bytes, doc_type: str) -> Dict[str, Any]:
    """
    Sends a single source document page image (as bytes) to Gemini Pro Vision
    and requests key-value data extraction using a type-specific prompt.
    Args:
        image_bytes: The image content as bytes.
        doc_type: The classified document type (e.g., "W-2", "Profit and Loss Statement").
    Returns:
        A dictionary of extracted key-value pairs.
    """
    image_part = {"mime_type": "image/png", "data": image_bytes}

    # Select prompt based on document type
    if doc_type == "W-2":
        prompt = PROMPT_W2
    elif doc_type == "Profit and Loss Statement":
        prompt = PROMPT_PNL
    elif doc_type == "Cash Flow Statement":
        prompt = PROMPT_CASHFLOW
    elif doc_type == "Invoice":
        prompt = PROMPT_INVOICE
    elif doc_type == "1099-NEC":
        prompt = PROMPT_1099_NEC
    elif doc_type == "1099-INT":
        prompt = PROMPT_1099_INT
    elif doc_type == "1099-DIV":
        prompt = PROMPT_1099_DIV
    elif doc_type == "1099-MISC":
        prompt = PROMPT_1099_MISC
    # Add elif for other specific types (Receipt, Insurance Policy, etc.)
    else: # Default to generic prompt for "Other" or unhandled specific types
        prompt = PROMPT_GENERIC
        if doc_type != "Other":
             prompt = prompt.replace("a page from a tax form", f"a page from a document classified as '{doc_type}'", 1)

    # Construct the prompt for Gemini - focused on Key-Value Extraction
    # --- Existing prompt definition REMOVED as we select above --- 
    
    # --- Existing Gemini API call and JSON parsing logic --- 
    try:
        # print(f"DEBUG: Using prompt for type '{doc_type}':\n{prompt[:200]}...") # Optional debug
        response = model.generate_content([prompt, image_part], stream=False)
        response.resolve() # Ensure completion

        # Robust JSON parsing (similar to blank form extractor)
        raw_text = response.text.strip()
        json_text = raw_text
        if raw_text.startswith("```json"):
            json_text = raw_text[7:]
        if raw_text.endswith("```"):
            json_text = json_text[:-3]
        json_text = json_text.strip()

        if not json_text.startswith("{") or not json_text.endswith("}"):
             print(f"Warning: Gemini response does not appear to be a valid JSON object for doc_type {doc_type}:\n{raw_text}")
             json_start = raw_text.find('{')
             json_end = raw_text.rfind('}') + 1
             if json_start != -1 and json_end > json_start:
                  json_text = raw_text[json_start:json_end]
                  print("Attempting to parse extracted JSON substring.")
             else:
                  return {"error": "Failed to find valid JSON in Gemini response.", "raw_response": raw_text}

        parsed_json = json.loads(json_text)
        # Add the classified type for certainty if not extracted by model
        if "DocumentType" not in parsed_json:
            parsed_json["DocumentType"] = doc_type 
        return parsed_json

    except json.JSONDecodeError as json_err:
        print(f"Error decoding JSON from Gemini response for doc_type {doc_type}: {json_err}")
        print(f"Raw response text:\n{raw_text}")
        return {"error": f"JSONDecodeError: {json_err}", "raw_response": raw_text}
    except Exception as e:
        print(f"Error during Gemini API call or processing for doc_type {doc_type}: {e}")
        # ... (Existing detailed error feedback logic) ...
        raw_response_text = "N/A"
        feedback_info = {}
        try:
            if 'raw_text' in locals(): raw_response_text = raw_text
            elif response: raw_response_text = response.text
        except Exception: pass
        try:
            if response and response.prompt_feedback: feedback_info['prompt_feedback'] = str(response.prompt_feedback)
            if response and response.candidates and response.candidates[0].finish_reason: feedback_info['finish_reason'] = str(response.candidates[0].finish_reason)
            if response and response.candidates[0].finish_reason != "STOP": feedback_info['safety_ratings'] = str(response.candidates[0].safety_ratings)
        except Exception as feedback_err: feedback_info['feedback_error'] = str(feedback_err)
        return {"error": f"Gemini API call failed: {e}", "raw_response": raw_response_text, "feedback": feedback_info}


# --- Main PDF Processing Function ---

def extract_data_from_source_pdf(pdf_path: str, doc_type: str) -> Dict[str, Any]: # Added doc_type argument
    """
    Extracts key-value data from a source PDF (W-2, 1099, receipt, etc.)
    using Gemini Pro Vision by analyzing each page with a type-specific prompt.
    Args:
        pdf_path: Path to the source PDF file.
        doc_type: The classified document type string.
    Returns:
        A dictionary where keys are page numbers (1-indexed) and
        values are the dictionaries of extracted data returned by Gemini for that page.
        Handles potential errors during conversion or extraction.
    """
    if not os.path.exists(pdf_path):
        return {"error": f"Source PDF file not found: {pdf_path}"}

    print(f"Starting source document data extraction for: {pdf_path} (Type: {doc_type})") # Log type
    all_pages_data = {}
    images: Optional[List[Image.Image]] = None

    try:
        images = pdf_to_images(pdf_path)
        print(f"Converted PDF to {len(images)} page image(s).")

        for i, page_image in enumerate(images):
            page_num = i + 1
            print(f"Analyzing page {page_num}/{len(images)}...")
            page_data = {}
            try:
                image_bytes = image_to_byte_array(page_image)
                # Pass doc_type to the page extraction function
                page_data = extract_data_from_source_document_page(image_bytes, doc_type)
                all_pages_data[f"page_{page_num}"] = page_data
                # time.sleep(1) # Optional delay
            except Exception as page_err:
                print(f"Error processing page {page_num} of {pdf_path}: {page_err}")
                all_pages_data[f"page_{page_num}"] = {"error": f"Failed to process page: {page_err}"}
            finally:
                if page_image:
                    page_image.close()

    except Exception as conv_err:
        print(f"Failed PDF-to-image conversion for {pdf_path}: {conv_err}")
        return {"error": f"Failed PDF-to-image conversion: {conv_err}"}
    finally:
        if images:
             for img in images:
                 try: img.close()
                 except Exception: pass

    print(f"Source document analysis complete for: {pdf_path}")
    return all_pages_data


# --- Example Usage ---
if __name__ == '__main__':
    # --- Configuration ---
    # 1. Ensure .env file with GEMINI_API_KEY exists in the project root.
    # 2. Ensure poppler is installed and in PATH.
    # 3. Install requirements: pip install google-generativeai pdf2image Pillow python-dotenv
    # 4. CHANGE THE `example_source_pdf` path below to point to an actual W-2, 1099, receipt PDF etc.

    example_source_pdf = 'test_forms/example_w2.pdf' # <--- CHANGE THIS PATH
    example_doc_type = "W-2" # <--- PROVIDE EXAMPLE TYPE FOR TESTING
    output_dir = 'output' # Directory to save the JSON results

    # --- Execution ---
    if not os.path.exists(example_source_pdf):
        print(f"Error: Example source PDF not found at '{example_source_pdf}'")
        print("Please update the 'example_source_pdf' variable in the script with a valid path.")
    else:
        os.makedirs(output_dir, exist_ok=True)

        print(f"Running source document data extraction for: {example_source_pdf} (Assuming type: {example_doc_type})")
        extracted_data = extract_data_from_source_pdf(example_source_pdf, example_doc_type)

        # Determine output filename
        base_filename = os.path.splitext(os.path.basename(example_source_pdf))[0]
        output_filename = os.path.join(output_dir, f"{base_filename}_gemini_extracted_data.json")

        print(f"\nSaving extracted data to: {output_filename}")
        try:
            with open(output_filename, 'w') as f:
                json.dump(extracted_data, f, indent=4)
            print("Successfully saved data.")
        except IOError as e:
            print(f"Error saving data to JSON file: {e}")
        except TypeError as e:
             print(f"Error: Data structure cannot be serialized to JSON: {e}")
             print("Printing raw data instead:")
             print(extracted_data)

        # Optionally print a summary
        print("\nExtraction Summary:")
        total_items = 0
        has_errors = False
        for page, data in extracted_data.items():
            if isinstance(data, dict) and 'error' in data:
                print(f"- {page}: Error - {data['error']}")
                has_errors = True
            elif isinstance(data, dict):
                 page_items = len(data) # Count keys in the extracted data dict
                 print(f"- {page}: Success - Extracted {page_items} key-value pairs.")
                 total_items += page_items
            else:
                 print(f"- {page}: Unknown status or invalid data structure.")
                 has_errors = True

        if not has_errors:
             print(f"\nTotal key-value pairs extracted across all pages: {total_items}")
        else:
            print("\nExtraction completed with errors on one or more pages.") 