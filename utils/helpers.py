"""
Utility functions for the tax automation pipeline.
"""

import json
import importlib
from typing import Dict, Any, List, Optional
import os
from pathlib import Path
from prefect import get_run_logger # Import Prefect logger at the top level

# Import PDF inspection library
try:
    import fitz # PyMuPDF
    PYMUPDF_AVAILABLE = True
except ImportError:
    print("Warning: PyMuPDF (fitz) library not found. PDF field mapping will rely solely on placeholder logic.")
    PYMUPDF_AVAILABLE = False
    fitz = None

# --- Placeholder Data --- 
# In a real scenario, these would load from actual files or a config service
_FORM_SCHEMAS = {}
_VALIDATION_RULES = {}
_PDF_FIELD_CACHE = {} # Cache inspected fields to avoid re-running

def load_schema(schema_path: str) -> Dict[str, Any]:
    """Loads a JSON schema from the specified path."""
    global _FORM_SCHEMAS
    schema_name = Path(schema_path).stem # e.g., '1040' or 'SchedC'
    if schema_name in _FORM_SCHEMAS:
        return _FORM_SCHEMAS[schema_name]

    try:
        with open(schema_path, 'r') as f:
            schema = json.load(f)
            _FORM_SCHEMAS[schema_name] = schema
            print(f"Loaded schema: {schema_path}")
            return schema
    except FileNotFoundError:
        print(f"Warning: Schema file not found at {schema_path}. Returning empty schema.")
        _FORM_SCHEMAS[schema_name] = {}
        return {}
    except json.JSONDecodeError:
        print(f"Warning: Error decoding JSON from {schema_path}. Returning empty schema.")
        _FORM_SCHEMAS[schema_name] = {}
        return {}

def load_validation_rules(rules_path: str) -> Any:
    """Loads validation rules from a Python module path."""
    global _VALIDATION_RULES
    module_name = Path(rules_path).stem # e.g., '1040_validation'
    if module_name in _VALIDATION_RULES:
        return _VALIDATION_RULES[module_name]

    try:
        # Convert file path to module path (e.g., rules/1040_validation.py -> rules.1040_validation)
        module_spec_path = rules_path.replace(os.path.sep, '.').replace('.py', '')
        rules_module = importlib.import_module(module_spec_path)
        _VALIDATION_RULES[module_name] = rules_module
        print(f"Loaded validation rules module: {module_spec_path}")
        return rules_module
    except ImportError as e:
        print(f"Warning: Could not import validation rules from {rules_path}: {e}. Returning None.")
        _VALIDATION_RULES[module_name] = None
        return None

def determine_target_forms(aggregated_data_by_type: Dict[str, List[Dict[str, Any]]]) -> List[str]:
    """
    Determines which target forms (e.g., '1040', 'SchedC', 'SchedE', '1040-SE') 
    to process based on document types and keys present in the aggregated, grouped data.
    """
    logger = get_run_logger()
    try:
        logger = get_run_logger()
    except ImportError:
        logger = None
        print("Prefect logger not available in determine_target_forms")

    targets = set(['1040']) # Use a set to avoid duplicates, always include 1040

    # --- Schedule C Determination ---
    sched_c_indicator_keys = {
        'NonemployeeCompensation', 'GrossReceipts', 'TotalRevenue', 'NetIncomeLoss', 
        'GrossProfit', 'ExpenseCategory', 'VendorName', 'CostOfGoodsSold', 
        'TotalOperatingExpenses', 'BusinessName', 'PrincipalBusinessActivity'
    }
    has_sched_c_hints = False
    if "Profit and Loss Statement" in aggregated_data_by_type or "Invoice" in aggregated_data_by_type or "Receipt" in aggregated_data_by_type or "1099-NEC" in aggregated_data_by_type:
         for doc_type, doc_list in aggregated_data_by_type.items():
             if doc_type in ["Profit and Loss Statement", "Invoice", "Receipt", "1099-NEC"]:
                 for doc_data in doc_list:
                     if any(key in doc_data for key in sched_c_indicator_keys):
                         has_sched_c_hints = True
                         break
             if has_sched_c_hints: break

    if has_sched_c_hints:
        targets.add('SchedC')
        log_msg = "Schedule C indicators found. Adding 'SchedC' to target forms."
        if logger: logger.info(log_msg)
        else: print(log_msg)
        
        # --- Add 1040-SE if SchedC is present ---
        targets.add('1040-SE')
        log_msg_se = "Schedule C present, adding '1040-SE' to target forms."
        if logger: logger.info(log_msg_se)
        else: print(log_msg_se)

    # --- Schedule E Determination ---
    sched_e_indicator_keys = {
        'RentalIncome', 'RoyaltyIncome', 'PartnershipIncome', 'SCorpIncome', 
        'PropertyAddress', 'RentalExpenses', 'PropertyTaxes', 'MortgageInterest' 
    }
    has_sched_e_hints = False
    if "Cash Flow Statement" in aggregated_data_by_type or "Profit and Loss Statement" in aggregated_data_by_type: # Or K-1s if classified
         for doc_type, doc_list in aggregated_data_by_type.items():
              # Check relevant doc types OR specific keys even in other docs
             if doc_type in ["Cash Flow Statement", "Profit and Loss Statement"]: # Add K-1 etc. later
                 for doc_data in doc_list:
                     if any(key in doc_data for key in sched_e_indicator_keys):
                         has_sched_e_hints = True
                         break
             if has_sched_e_hints: break
             
    if has_sched_e_hints:
        targets.add('SchedE')
        log_msg = "Schedule E indicators found. Adding 'SchedE' to target forms."
        if logger: logger.info(log_msg)
        else: print(log_msg)
        
    # --- Add Logic for Other Forms/Schedules ---
    # Schedule 1: Additional Income and Adjustments
    schedule_1_indicator_keys = {
        # Part I - Additional Income
        'AlimonyReceived', 'BusinessIncomeLoss', 'OtherGainsLosses', 'RentalRealEstateIncomeLoss', 
        'FarmIncomeLoss', 'UnemploymentCompensation', 'OtherIncomeGamblingWinnings', 'PrizesAwards', 
        'StockOptions', 'AlaskaPermanentFundDividends',
        # Part II - Adjustments to Income
        'EducatorExpenses', 'CertainBusinessExpensesReservists', 'HealthSavingsAccountDeduction',
        'MovingExpensesMilitary', 'DeductibleSE Tax', 'SE HealthInsuranceDeduction',
        'SEP_SIMPLE_QualifiedPlans', 'AlimonyPaid', 'IRADeduction', 'StudentLoanInterestDeduction'
        # Add more specific keys as extraction improves
    }
    has_schedule_1_hints = False
    for doc_list in aggregated_data_by_type.values():
        for doc_data in doc_list:
            if any(key in doc_data for key in schedule_1_indicator_keys):
                has_schedule_1_hints = True
                break
        if has_schedule_1_hints: break
        
    # Also add Schedule 1 if forms feeding into it are present (Sched C, E, F, 1040-SE for deduction)
    if not has_schedule_1_hints and any(f in targets for f in ['SchedC', 'SchedE', '1040-SE']): # Add Sched F later
        has_schedule_1_hints = True
        log_msg_dep = "Adding 'Schedule 1' because dependent forms (Sched C/E/SE) are present."
        if logger: logger.info(log_msg_dep)
        else: print(log_msg_dep)
        
    if has_schedule_1_hints:
        targets.add('Schedule 1')
        log_msg = "Schedule 1 indicators found or dependency met. Adding 'Schedule 1' to target forms."
        if logger: logger.info(log_msg)
        else: print(log_msg)

    # Schedule 2: Additional Taxes
    schedule_2_indicator_keys = {
        # Direct Inputs (less common)
        'AlternativeMinimumTaxAmount', 'ExcessAdvancePTCRepaymentAmount',
        # Indicators from other forms (that might be extracted)
        'UnreportedSocialSecurityMedicareTax', 'AdditionalTaxOnIRAs', 
        'HouseholdEmploymentTaxesAmount', 'AdditionalMedicareTaxAmount', 
        'NetInvestmentIncomeTaxAmount', 'FirstTimeHomebuyerCreditRepayment'
    }
    has_schedule_2_hints = False
    # Check direct keys
    for doc_list in aggregated_data_by_type.values():
        for doc_data in doc_list:
            if any(key in doc_data for key in schedule_2_indicator_keys):
                has_schedule_2_hints = True
                break
        if has_schedule_2_hints: break
    # Check dependencies (SE Tax is very common)
    if not has_schedule_2_hints and '1040-SE' in targets:
         has_schedule_2_hints = True
         log_msg_dep = "Adding 'Schedule 2' because 1040-SE is present."
         if logger: logger.info(log_msg_dep)
         else: print(log_msg_dep)
         
    # TODO: Add checks if forms like 6251, 8962, 4137, 5329, Sch H, 8959, 8960, 5405 are implemented

    if has_schedule_2_hints:
        targets.add('Schedule 2')
        log_msg = "Schedule 2 indicators found or dependency met. Adding 'Schedule 2' to target forms."
        if logger: logger.info(log_msg)
        else: print(log_msg)

    # Schedule 3: Additional Credits and Payments
    schedule_3_indicator_keys = {
        # Part I - Nonrefundable Credits
        'ForeignTaxCreditAmount', 'ChildCareExpenses', 'EducationCreditsAmount',
        'RetirementSavingsContributionsCreditAmount', 'ResidentialEnergyCreditsAmount',
        # Part II - Other Payments and Refundable Credits
        'NetPremiumTaxCreditAmount', 'AmountPaidWithExtension', 'ExcessSocialSecurityTaxWithheld'
        # Add more specific keys if forms like 1116, 2441, 8863, 8880, 5695, 8962 are processed
    }
    has_schedule_3_hints = False
    # Check direct keys
    for doc_list in aggregated_data_by_type.values():
        for doc_data in doc_list:
            if any(key in doc_data for key in schedule_3_indicator_keys):
                has_schedule_3_hints = True
                break
        if has_schedule_3_hints: break
    # TODO: Add checks if forms like 1116, 2441, 8863, 8880, 5695, 8962 etc. are processed

    if has_schedule_3_hints:
        targets.add('Schedule 3')
        log_msg = "Schedule 3 indicators found. Adding 'Schedule 3' to target forms."
        if logger: logger.info(log_msg)
        else: print(log_msg)

    # Form 2441: Child and Dependent Care Expenses
    form_2441_indicator_keys = {
        'ChildCareExpenses', 'DependentCareProviderName', 'ProviderTaxID', 
        'DependentNameForCare', 'DependentSSNForCare', 'EmployerProvidedDependentCareBenefits'
    }
    has_form_2441_hints = False
    for doc_list in aggregated_data_by_type.values():
        for doc_data in doc_list:
            if any(key in doc_data for key in form_2441_indicator_keys):
                has_form_2441_hints = True
                break
        if has_form_2441_hints: break
        
    if has_form_2441_hints:
        targets.add('Form 2441')
        log_msg = "Form 2441 indicators found. Adding 'Form 2441' to target forms."
        if logger: logger.info(log_msg)
        else: print(log_msg)

    # Form 8812: Credits for Qualifying Children and Other Dependents
    # Check if dependent aggregation yielded results
    has_dependents = False
    if isinstance(aggregated_data_by_type.get('Dependents'), list) and aggregated_data_by_type['Dependents']: # Check if aggregation created the list
        has_dependents = True
    elif 'DependentName' in str(aggregated_data_by_type) or 'DependentSSN' in str(aggregated_data_by_type): # Fallback check raw keys
        has_dependents = True
        
    if has_dependents:
        targets.add('Form 8812')
        log_msg = "Dependent indicators found. Adding 'Form 8812' to target forms."
        if logger: logger.info(log_msg)
        else: print(log_msg)

    # Schedule A: Itemized Deductions
    schedule_a_indicator_keys = {
        'MedicalExpenses', 'StateAndLocalTaxes', 'SALT', 'RealEstateTaxes', 
        'PersonalPropertyTaxes', 'HomeMortgageInterest', 'InvestmentInterest', 
        'CharitableContributionsCash', 'CharitableContributionsNonCash'
    }
    has_schedule_a_hints = False
    # Check direct keys
    for doc_list in aggregated_data_by_type.values():
        for doc_data in doc_list:
            if any(key in doc_data for key in schedule_a_indicator_keys):
                has_schedule_a_hints = True
                break
        if has_schedule_a_hints: break
        
    if has_schedule_a_hints:
        targets.add('Schedule A')
        log_msg = "Schedule A indicators found. Adding 'Schedule A' to target forms."
        if logger: logger.info(log_msg)
        else: print(log_msg)

    final_targets = sorted(list(targets)) # Convert back to sorted list
    final_log = f"Final determined target forms: {final_targets}"
    if logger: logger.info(final_log)
    else: print(final_log)
    return final_targets

# Removed _decode_pdf_field_name as PyPDF2 seems to handle it

def get_pdf_field_mapping(template_path: str, schema_keys: List[str]) -> Dict[str, str]:
    """
    Maps schema keys to PDF field names using predefined mappings.
    Falls back to basic heuristics if a field is not in the mapping.
    """
    import os
    import json
    from pathlib import Path
    
    print(f"Using predefined field mapping for: {template_path}")
    print(f"Schema keys to map: {schema_keys}")
    
    # Determine which form we're dealing with based on the filename
    template_filename = os.path.basename(template_path)
    mapping_file = None
    
    if 'f1040_blank.pdf' in template_filename:
        mapping_file = os.path.join('mappings', '1040_field_mapping.json')
        form_type = '1040'
    elif 'f1040sc_blank.pdf' in template_filename:
        mapping_file = os.path.join('mappings', 'schedC_field_mapping.json')
        form_type = 'SchedC'
    else:
        print(f"Warning: Unknown template: {template_filename}. No predefined mapping available.")
        # Fall back to the old behavior for unknown forms
        # This is simplified from the original function
        if PYMUPDF_AVAILABLE:
            doc = fitz.open(template_path)
            field_names = set()
            for page in doc:
                widget = page.first_widget
                while widget:
                    if widget.field_name:
                        field_names.add(widget.field_name)
                    widget = widget.next
            doc.close()
            
            # Return very simple 1:1 mapping for first n fields
            pdf_fields = sorted(list(field_names))
            return {k: pdf_fields[i] for i, k in enumerate(schema_keys) if i < len(pdf_fields)}
        else:
            print("PyMuPDF not available and no predefined mapping exists.")
            return {}
    
    # Load the appropriate mapping file
    try:
        if os.path.exists(mapping_file):
            with open(mapping_file, 'r') as f:
                predefined_mapping = json.load(f)
            print(f"Loaded predefined mapping with {len(predefined_mapping)} fields for {form_type} form")
            
            # Create the actual mapping for the requested schema keys
            final_mapping = {}
            
            # Convert "dot" format to "underscore" format if needed
            for schema_key in schema_keys:
                # Test both the original key and one where dots are replaced with underscores
                flattened_key = schema_key.replace('.', '_')
                
                if schema_key in predefined_mapping:
                    final_mapping[schema_key] = predefined_mapping[schema_key]
                elif flattened_key in predefined_mapping:
                    final_mapping[schema_key] = predefined_mapping[flattened_key]
                    print(f"Mapped using flattened key: {flattened_key} -> {schema_key}")
                else:
                    print(f"Warning: No mapping found for schema key: {schema_key} (or {flattened_key})")
            
            print(f"Mapped {len(final_mapping)} out of {len(schema_keys)} requested fields")
            # Print the final mapping for debugging
            if final_mapping:
                print(f"Final mapping sample: {list(final_mapping.items())[:3]}")
            return final_mapping
        else:
            print(f"Warning: Mapping file not found: {mapping_file}")
            return {}
    except Exception as e:
        print(f"Error loading mapping file: {e}")
        return {}

# --- Potentially add more helpers ---
# - Text cleaning/normalization utilities
# - Date parsing utilities
# - Numeric value cleaning utilities
# - Function to save intermediate results securely 