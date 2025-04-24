"""
Task for mapping extracted information to target form schema and normalizing data.
"""

from typing import Dict, Any, List, Optional
from prefect import task, get_run_logger
import datetime
import re
from pathlib import Path # Added for recursive normalize fix
from decimal import Decimal, InvalidOperation # Added for robust numeric conversion
import json # Add json import
from collections import Counter # Import Counter

# --- Helper Functions for Calculations ---

def _get_field_value(structure: Dict[str, Any], field_name: str) -> Any:
    """Safely retrieves the 'value' for a given field_name from the structure."""
    for page_key, page_content in structure.items():
        if isinstance(page_content, dict) and "fields" in page_content and isinstance(page_content["fields"], list):
            for field_def in page_content["fields"]:
                if isinstance(field_def, dict) and field_def.get("field_name") == field_name:
                    return field_def.get("value")
    return None

def _get_numeric_value(structure: Dict[str, Any], field_name: str, default: float = 0.0) -> float:
    """Safely retrieves and converts a field's value to float, returning default if invalid/missing."""
    value = _get_field_value(structure, field_name)
    if value is None:
        return default
    try:
        # Handle potential strings with currency symbols, commas, parentheses
        if isinstance(value, str):
            cleaned_value = re.sub(r'[$,]', '', value) # Remove $ and ,
            if cleaned_value.startswith('(') and cleaned_value.endswith(')'):
                cleaned_value = '-' + cleaned_value[1:-1] # Handle negatives in parentheses
            return float(cleaned_value)
        elif isinstance(value, (int, float, Decimal)):
            return float(value)
        else:
            return default
    except (ValueError, TypeError):
        return default

def _set_field_value(structure: Dict[str, Any], field_name: str, value: Any, source: str = "Calculated"):
    """Safely sets the 'value' and 'sources' for a given field_name in the structure."""
    logger = get_run_logger()
    found = False
    for page_key, page_content in structure.items():
        if isinstance(page_content, dict) and "fields" in page_content and isinstance(page_content["fields"], list):
            for field_def in page_content["fields"]:
                if isinstance(field_def, dict) and field_def.get("field_name") == field_name:
                    field_def["value"] = value
                    field_def["sources"] = [source] # Overwrite sources for calculated fields
                    found = True
                    break
        if found:
            break
    if not found:
        logger.warning(f"Could not find field '{field_name}' to set calculated value.")

# --- End Helper Functions for Calculations ---

# --- Helper Functions for Aggregation ---
def _aggregate_data_for_form(
    aggregated_data_by_type: Dict[str, List[Dict[str, Any]]],
    relevant_doc_types: List[str],
    summable_keys: set,
    modal_keys: set,
    list_keys: set,
    proprietor_keys: set
) -> Dict[str, Any]:
    """ 
    Aggregates data from specified document types based on key type (sum, mode, list).
    Tracks sources for each aggregated value.
    Returns a dictionary like {"AggregatedKey": {"value": ..., "sources": [...]}}.
    """
    logger = get_run_logger()
    final_aggregated = {}
    all_values_for_modal_keys = {key: [] for key in modal_keys}
    aggregated_lists = {key: [] for key in list_keys}
    proprietor_data = {}

    logger.info(f"Starting aggregation for doc types: {relevant_doc_types}")

    for doc_type in relevant_doc_types:
        if doc_type in aggregated_data_by_type:
            logger.debug(f"Processing {len(aggregated_data_by_type[doc_type])} documents of type '{doc_type}'")
            for document_data in aggregated_data_by_type[doc_type]:
                doc_source = "UnknownSource"
                # Attempt to find a primary source identifier within the document data
                # This assumes individual document data items might have a source key
                # or we fall back to using a placeholder based on a common key like EmployeeSSN
                # This part might need refinement based on the exact structure of `document_data`
                if isinstance(document_data, dict) and "source" in document_data:
                    doc_source = document_data["source"] # Ideal case
                elif "EmployeeSSN" in document_data and isinstance(document_data["EmployeeSSN"], dict):
                     doc_source = f"Doc linked to SSN: {document_data['EmployeeSSN'].get('value', 'N/A')}" # Fallback
                
                is_proprietor_doc = any(prop_key in document_data for prop_key in proprietor_keys)
                
                for key, value_source_dict in document_data.items():
                    # Ensure value_source_dict is the expected format
                    if not isinstance(value_source_dict, dict) or 'value' not in value_source_dict or 'source' not in value_source_dict:
                        # logger.warning(f"Skipping unexpected data format for key '{key}' in {doc_source}. Expected dict with 'value' and 'source'. Got: {type(value_source_dict)}")
                        continue # Skip if the format is not { "value": ..., "source": ... }
                    
                    value = value_source_dict.get('value')
                    source = value_source_dict.get('source', doc_source) # Use specific source if available, else doc source
                    cleaned_value = _clean_string(value)
                    if cleaned_value is None: continue

                    # --- Summation --- 
                    if key in summable_keys:
                        agg_info = final_aggregated.setdefault(key, {"value": 0.0, "sources": set()})
                        try:
                            # Use Decimal for precision during summation
                            numeric_value = Decimal(re.sub(r'[$,()]', '', cleaned_value))
                            current_total = Decimal(agg_info.get("value", 0.0))
                            agg_info["value"] = float(current_total + numeric_value) # Store as float finally
                            agg_info["sources"].add(source)
                        except (InvalidOperation, ValueError, TypeError):
                            logger.warning(f"Could not convert '{cleaned_value}' to number for summable key '{key}' from {source}. Skipping.")
                    
                    # --- Modal --- 
                    elif key in modal_keys:
                         all_values_for_modal_keys[key].append((cleaned_value, source))

                    # --- List --- 
                    elif key in list_keys:
                        # Store as dicts to preserve source for each list item
                        aggregated_lists[key].append({"value": cleaned_value, "source": source})

                    # --- Proprietor Info (Treat similar to Modal but separate storage) ---
                    elif key in proprietor_keys and is_proprietor_doc:
                        proprietor_data.setdefault(key, []).append((cleaned_value, source))

    # --- Post-aggregation Processing --- 

    # Finalize Modal Keys (Find most common value, preserve all sources for that value)
    for key, value_source_list in all_values_for_modal_keys.items():
        if value_source_list:
            values = [item[0] for item in value_source_list]
            try:
                most_common_value = Counter(values).most_common(1)[0][0]
                # Collect all sources associated with the most common value
                sources = {item[1] for item in value_source_list if item[0] == most_common_value}
                final_aggregated[key] = {"value": most_common_value, "sources": sources}
                logger.debug(f"Modal aggregation for '{key}': Chose '{most_common_value}'")
            except IndexError:
                logger.warning(f"Could not determine modal value for key '{key}' (empty list?).")
        else:
             logger.debug(f"No values found for modal key '{key}'.")
             
    # Finalize Proprietor Keys (Similar to Modal)
    for key, value_source_list in proprietor_data.items():
        if value_source_list:
            values = [item[0] for item in value_source_list]
            try:
                most_common_value = Counter(values).most_common(1)[0][0]
                sources = {item[1] for item in value_source_list if item[0] == most_common_value}
                final_aggregated[key] = {"value": most_common_value, "sources": sources}
                logger.debug(f"Proprietor aggregation for '{key}': Chose '{most_common_value}'")
            except IndexError:
                logger.warning(f"Could not determine proprietor value for key '{key}' (empty list?).")
        else:
             logger.debug(f"No values found for proprietor key '{key}'.")

    # Finalize List Keys (Store the collected list of dicts)
    for key, value_list in aggregated_lists.items():
        if value_list:
            final_aggregated[key] = value_list # Store the list of {"value": ..., "source": ...}
            logger.debug(f"List aggregation for '{key}': Collected {len(value_list)} items.")
        else:
            logger.debug(f"No values found for list key '{key}'.")
            
    # Convert source sets to sorted lists for consistent output
    for key in final_aggregated:
        if isinstance(final_aggregated[key], dict) and "sources" in final_aggregated[key] and isinstance(final_aggregated[key]["sources"], set):
            final_aggregated[key]["sources"] = sorted(list(final_aggregated[key]["sources"]))
            
    logger.info(f"Aggregation complete. Final keys: {list(final_aggregated.keys())}")
    return final_aggregated
# --- End Helper Functions for Aggregation ---

# Assume schema is loaded via helpers
# Assume ML mapping models are loaded if used

# --- Helper function to create nested dictionary (COMMENTED OUT - No longer needed) ---
# def _nest_data(flat_data: Dict[str, Any]) -> Dict[str, Any]:
#     """Converts a flat dictionary with dot-notation keys into a nested dictionary."""
#     nested_dict = {}
#     for flat_key, value in flat_data.items():
#         keys = flat_key.split('.')
#         d = nested_dict
#         for i, key in enumerate(keys):
#             if i == len(keys) - 1:
#                 # Last key, assign value
#                 d[key] = value
#             else:
#                 # Navigate or create nested dict
#                 d = d.setdefault(key, {})
#                 # Handle case where an intermediate key was already assigned a non-dict value (error or overwrite?)
#                 if not isinstance(d, dict):
#                     print(f"Warning: Key path conflict while nesting data for '{flat_key}'. Overwriting previous value at '{key}'.")
#                     # Overwrite previous value to create nested dict
#                     # Find parent dict to perform overwrite
#                     parent_d = nested_dict
#                     for j in range(i):
#                         parent_d = parent_d[keys[j]]
#                     parent_d[key] = {}
#                     d = parent_d[key]
#                     
#     return nested_dict

# --- Helper for basic string cleanup (Still needed) ---
def _clean_string(value: Any) -> Optional[str]:
    if value is None: return None
    return str(value).strip()

# --- Mapping function (Maps FINAL aggregated keys -> Gemini field names) ---
def _map_aggregated_to_gemini_fields(aggregated_form_data: Dict[str, Any], target_form: str) -> Dict[str, Any]:
    """Maps FINAL AGGREGATED data keys (which contain value+sources) to the target form's Gemini field names."""
    print(f"Mapping FINAL aggregated {target_form} data to Gemini field names...")
    mapped_data = {}

    # --- Define Mappings --- 
    # Keys: Keys used in final_aggregated_values (e.g., WagesTipsOtherComp, Line1_GrossReceiptsSales_INPUT)
    # Values: field_name from output/f1040*_blank_gemini_extracted_fields.json
    SOURCE_TO_TARGET_MAP_1040 = {
        # Aggregated W-2 Data -> 1040 Gemini field_name
        "WagesTipsOtherComp": "Income_1z", 
        "FederalIncomeTaxWithheld": "Line25a_FormW2", 
        "EmployeeSSN": "YourSocialSecurityNumber",
        "EmployeeName": ["FirstNameInitial", "LastName"], # Special handling
        "EmployeeAddress": "HomeAddress", 
        # Filing Status (Assuming extracted/aggregated from source)
        "FilingStatus": "FilingStatus", 
        # Other Payments/Withholding (Assuming aggregated from relevant sources)
        "Aggregated1099Withholding": "Line25b_Form1099",
        "AggregatedOtherWithholding": "Line25c_OtherForms", # e.g., Form 2439 box 2
        "EstimatedTaxPaymentsMade": "Line26_EstimatedTaxPayments",
        # Add other mappings needed for 1040 calculations (Interest, Dividends, etc.)
        # Example: Assuming keys like 'TotalTaxableInterest', 'TotalOrdinaryDividends' exist after aggregation
        "TotalTaxableInterest": "Line2b_TaxableInterest",
        "TotalOrdinaryDividends": "Line3b_OrdinaryDividends",
        "TotalTaxableIRADistributions": "Line4b_TaxableIRADistributions",
        "TotalTaxablePensionsAnnuities": "Line5b_TaxablePensionsAnnuities",
        "TotalTaxableSocialSecurity": "Line6b_TaxableSocialSecurity",
        "TotalCapitalGainLoss": "Line7_CapitalGainLoss", # Likely comes from Sched D aggregation/calc
    }
    SOURCE_TO_TARGET_MAP_SchedC = {
        # Aggregated Basic Info -> Sched C Gemini field_name
        "BusinessName": "BusinessName",
        "PrincipalBusinessActivity": "PrincipalBusiness", 
        "EmployerIdentificationNumber": "EIN", 
        "BusinessAddress": "BusinessAddress",
        "BusinessCityStateZip": "BusinessCity", 
        "EmployeeName": "NameOfProprietor", # Proprietor Info
        "EmployeeSSN": "SSN",
        # Aggregated Income -> Sched C Gemini field_name
        "Line1_GrossReceiptsSales_INPUT": "Line1_GrossReceiptsSales",
        "ReturnsAllowances": "Line2_ReturnsAllowances",
        # Aggregated COGS -> Sched C Gemini field_name
        "CostOfGoodsSold": "Line4_CostOfGoodsSold",
        # Aggregated Expenses -> Sched C Gemini field_name
        "AdvertisingExpense": "Line8_Advertising",
        "CarTruckExpense": "Line9_CarTruckExpenses",
        "CommissionsFeesExpense": "Line10_CommissionsFees",
        "ContractLaborExpense": "Line11_ContractLabor",
        "DepletionExpense": "Line12_Depletion",
        "DepreciationExpense": "Line13_DepreciationSection179",
        "EmployeeBenefitExpense": "Line14_EmployeeBenefitPrograms",
        "InsuranceExpense": "Line15_Insurance",
        "InterestMortgageExpense": "Line16a_InterestMortgage",
        "InterestOtherExpense": "Line16b_InterestOther",
        "LegalProfessionalExpense": "Line17_LegalProfessionalServices",
        "OfficeExpense": "Line18_OfficeExpense", 
        "PensionProfitSharingExpense": "Line19_PensionProfitSharing",
        "RentLeaseVehicleExpense": "Line20a_RentLeaseVehicles",
        "RentLeaseOtherExpense": "Line20b_RentLeaseOther",
        "RepairsMaintenanceExpense": "Line21_RepairsMaintenance",
        "SuppliesExpense": "Line22_Supplies",
        "TaxesLicensesExpense": "Line23_TaxesLicenses",
        "TravelExpense": "Line24a_Travel",
        "MealsExpense": "Line24b_DeductibleMeals", 
        "UtilitiesExpense": "Line25_Utilities",
        "WagesExpense": "Line26_Wages",
    }
    # Map source keys to Schedule E Gemini field_names
    SOURCE_TO_TARGET_MAP_SchedE = {
         # Basic Info (From Cash Flow or P&L?)
         "PropertyAddress": "SchedE_Line1a_AddressA", # Map aggregated address to Property A address
         # Income (From Cash Flow or P&L)
         "SchedE_TotalRents": "SchedE_Line3_RentsReceivedA", # Use the aggregated key
         # Expenses (Only Total Available from Cash Flow currently)
         "TotalOperatingExpenses": "SchedE_Line19_OtherExpenseA", # Map total expenses to Other Expenses A
         # We need specific expense keys extracted to map lines 5-18, 20
         # "InsuranceExpense": "SchedE_Line9_InsuranceA",
         # "RepairsExpense": "SchedE_Line14_RepairsA", 
         # "TaxesExpense": "SchedE_Line16_TaxesA",
         # "UtilitiesExpense": "SchedE_Line17_UtilitiesA",
    }
    # Map source keys to 1040-SE Gemini field_names
    SOURCE_TO_TARGET_MAP_1040SE = {
        # Proprietor Info (From W-2 Aggregation)
        "EmployeeName": "SE_NameShownOnReturn",
        "EmployeeSSN": "SE_SSN",
        # Main Input (From Sched C / P&L Aggregation)
        "NetIncomeLoss": "SE_Line2_NetProfitLoss", # Map Sched C result key
        # Map W-2 Wages for Line 8a calculation
        "WagesTipsOtherComp": "SE_Line8a_TotalWages" # Add mapping for W-2 wages
    }

    current_map = {}
    if target_form == '1040':
        current_map = SOURCE_TO_TARGET_MAP_1040
    elif target_form == 'SchedC':
        current_map = SOURCE_TO_TARGET_MAP_SchedC
    elif target_form == '1040-SE':
         current_map = SOURCE_TO_TARGET_MAP_1040SE
    elif target_form == 'SchedE':
         current_map = SOURCE_TO_TARGET_MAP_SchedE
    elif target_form == 'Schedule 1':
         # Define Schedule 1 Mappings (Add more as needed)
         SOURCE_TO_TARGET_MAP_Schedule1 = {
             # Part I Inputs (Examples)
             "AlimonyReceived": "Sch1_Line1_AlimonyReceived",
             "UnemploymentCompensation": "Sch1_Line7_UnemploymentComp",
             "AlaskaPermanentFundDividends": "Sch1_Line8b_AlaskaPermanentFund",
             "OtherIncomeDescription": "Sch1_Line8z_OtherIncomeDescription", # Assuming text desc field
             "OtherIncomeAmount": "Sch1_Line8z_OtherIncomeAmount",
             # Part II Inputs (Examples)
             "EducatorExpenses": "Sch1_Line11_EducatorExpenses",
             "StudentLoanInterestDeduction": "Sch1_Line21_StudentLoanInterest",
             "AlimonyPaid": "Sch1_Line19a_AlimonyPaid",
             "AlimonyRecipientSSN": "Sch1_Line19b_RecipientSSN",
         }
         current_map = SOURCE_TO_TARGET_MAP_Schedule1
    elif target_form == 'Schedule 2':
        # Define Schedule 2 Mappings (Mostly from other forms, few direct)
        SOURCE_TO_TARGET_MAP_Schedule2 = {
            # Direct Inputs (Examples, if extracted from source)
            "AlternativeMinimumTaxAmount": "Sch2_Line2_AMT",
            "ExcessAdvancePTCRepaymentAmount": "Sch2_Line1a_ExcessAdvPTC"
        }
        current_map = SOURCE_TO_TARGET_MAP_Schedule2
    elif target_form == 'Schedule 3':
        # Define Schedule 3 Mappings
        SOURCE_TO_TARGET_MAP_Schedule3 = {
            # Part I Inputs (Examples - many come from other forms)
            "ForeignTaxCreditAmount": "Sch3_Line1_ForeignTaxCredit", # From Form 1116
            "ChildDependentCareCreditAmount": "Sch3_Line2_ChildCareCredit", # From Form 2441
            "EducationCreditsAmount": "Sch3_Line3_EducationCredits", # From Form 8863
            "RetirementSavingsContributionsCreditAmount": "Sch3_Line4_RetirementSavingsCredit", # From Form 8880
            "ResidentialCleanEnergyCreditAmount": "Sch3_Line5a_ResidentialCleanEnergy", # From Form 5695
            "EnergyEfficientHomeImprovementCreditAmount": "Sch3_Line5b_EnergyEfficientHomeImprovement", # From Form 5695
            # Part II Inputs (Examples)
            "NetPremiumTaxCreditAmount": "Sch3_Line9_NetPremiumTaxCredit", # From Form 8962
            "AmountPaidWithExtension": "Sch3_Line10_AmountPaidWithExtension", # From Form 4868/Direct Input
            "ExcessSocialSecurityTaxWithheld": "Sch3_Line11_ExcessSSTaxWithheld", # Calculated or from W-2s
            "CreditForFuelTaxAmount": "Sch3_Line12_CreditForFuelTax", # From Form 4136
            "CreditFromForm2439": "Sch3_Line13a_CreditFromForm2439" # From Form 2439
        }
        current_map = SOURCE_TO_TARGET_MAP_Schedule3
    elif target_form == 'Form 2441':
        # Define Form 2441 Mappings
        SOURCE_TO_TARGET_MAP_Form2441 = {
            # Part I - Provider Info (Needs specific handling for multiple providers)
            "DependentCareProviderName": "F2441_Line1a_ProviderName",
            "ProviderAddress": "F2441_Line1b_ProviderAddress",
            "ProviderTaxID": "F2441_Line1c_ProviderTIN",
            "ProviderAmountPaid": "F2441_Line1e_AmountPaid",
            # Part II - Dependent/Expense Info
            "DependentNameForCare": "F2441_Line2a_DependentName",
            "DependentSSNForCare": "F2441_Line2b_DependentSSN",
            "ChildCareExpenses": "F2441_Line2c_QualifiedExpenses", # Summed Expenses
            # Part III - Employer Benefits
            "EmployerProvidedDependentCareBenefits": "F2441_Line12_EmployerBenefits" # From W-2 Box 10
        }
        current_map = SOURCE_TO_TARGET_MAP_Form2441
    elif target_form == 'Form 8812':
        # Define Form 8812 Mappings (Minimal direct needed, mostly uses aggregated dependents)
        SOURCE_TO_TARGET_MAP_Form8812 = {
            # Primarily uses AGI and dependent list
        }
        current_map = SOURCE_TO_TARGET_MAP_Form8812
    elif target_form == 'Schedule A':
        # Define Schedule A Mappings
        SOURCE_TO_TARGET_MAP_ScheduleA = {
            # Medical Expenses
            "MedicalExpenses": "SchA_Line1_MedicalDentalExpenses",
            # Taxes You Paid
            "StateAndLocalTaxes": "SchA_Line5a_StateLocalTaxes",
            "RealEstateTaxes": "SchA_Line5b_RealEstateTaxes",
            "PersonalPropertyTaxes": "SchA_Line5c_PersonalPropertyTaxes",
            # Interest You Paid
            "HomeMortgageInterest": "SchA_Line8a_HomeMortgageInterest", # Needs Form 1098 check?
            "InvestmentInterest": "SchA_Line9_InvestmentInterest", # Needs Form 4952
            # Gifts to Charity
            "CharitableContributionsCash": "SchA_Line11_ContributionsCash",
            "CharitableContributionsNonCash": "SchA_Line12_ContributionsOther", # Needs Form 8283
            # Casualty and Theft Losses (Needs Form 4684)
            # Other Itemized Deductions
        }
        current_map = SOURCE_TO_TARGET_MAP_ScheduleA

    # Apply mapping using the aggregated data (which includes sources)
    for agg_key, aggregated_value_with_source in aggregated_form_data.items():
        # Check if this aggregated key exists in the mapping for the current form
        if agg_key in current_map:
            target_key_or_list = current_map[agg_key]
            
            if isinstance(target_key_or_list, list): # Handle name split
                 if agg_key == "EmployeeName" and isinstance(aggregated_value_with_source, dict):
                     full_name = str(aggregated_value_with_source.get('value', ''))
                     sources = aggregated_value_with_source.get('sources', [])
                     parts = full_name.split(maxsplit=1)
                     first_initial = _clean_string(parts[0][0]) if parts else ''
                     last_name = _clean_string(parts[1]) if len(parts) > 1 else ''
                     if "FirstNameInitial" in target_key_or_list:
                          mapped_data["FirstNameInitial"] = {"value": first_initial, "sources": sources}
                     if "LastName" in target_key_or_list:
                          mapped_data["LastName"] = {"value": last_name, "sources": sources}
            else:
                # Pass the entire aggregated structure (value + sources)
                mapped_data[target_key_or_list] = aggregated_value_with_source 
                # print(f"Mapped aggregated key: {agg_key} -> {target_key_or_list}")

    print(f"Mapping aggregated data to {len(mapped_data)} Gemini fields complete.")
    return mapped_data

# --- Calculation Logic ---
def _perform_calculations(populated_structure: Dict[str, Any], form_type: str, populated_structures_cache: Dict[str, Any], final_aggregated_values: Dict[str, Any]):
    """Applies IRS calculation rules to the populated structure."""
    logger = get_run_logger()
    logger.info(f"Performing calculations for {form_type}...")

    # --- Calculation blocks for each form type ---
    if form_type == '1040':
        # --- Update Line 12 calculation --- 
        logger.info("Updating 1040 calculations with Schedule A results...")
        sched_a_structure = populated_structures_cache.get('Schedule A', {}) 
        # Fetch the *calculated* total from Sch A, if it exists
        line17_itemized_total = 0.0 
        if sched_a_structure: # Check if Schedule A was processed and cached
            line17_itemized_total = _get_numeric_value(sched_a_structure, "SchA_Line17_TotalItemizedDeductions")
        else:
            logger.info("Schedule A not found in cache. Using Standard Deduction.")

        # Line 12: Standard Deduction or Itemized Deductions (Sched A)
        filing_status = _get_field_value(populated_structure, "FilingStatus") 
        std_deduction_map = {'Single': 13850, 'MarriedFilingJointly': 27700, 'MarriedFilingSeparately': 13850, 'HeadOfHousehold': 20800, 'QualifyingWidow(er)': 27700} # Needs config
        line12_std_deduction = std_deduction_map.get(filing_status, 13850)
        
        use_itemized = Decimal(line17_itemized_total) > Decimal(line12_std_deduction)
        line12_final_deduction = Decimal(line17_itemized_total) if use_itemized else Decimal(line12_std_deduction)
        _set_field_value(populated_structure, "Line12_DeductionAmount", float(line12_final_deduction))
        _set_field_value(populated_structure, "Line12_UsedItemized", use_itemized) # Add flag if needed
        logger.info(f"Recalculated 1040 Line 12 (Deduction) using Sch A ({line17_itemized_total:.2f}) vs Standard ({line12_std_deduction:.2f}): Final = {line12_final_deduction:.2f} ({'Itemized' if use_itemized else 'Standard'})")

        # --- Recalculate subsequent lines dependent on Line 12 deduction --- 
        # Line 14 = Line 12 + Line 13
        line13 = _get_numeric_value(populated_structure, "Line13_QualifiedBusinessIncomeDeduction") # Placeholder
        line14 = line12_final_deduction + Decimal(line13)
        _set_field_value(populated_structure, "Line14_TotalDeductions", float(line14))
        logger.info(f"Recalculated 1040 Line 14 (Total Deductions) using Sch A: {line14:.2f}")

        # Line 15 = Line 11 - Line 14
        line11_agi = _get_numeric_value(populated_structure, "Line11_AdjustedGrossIncome") # Already calculated
        line15_taxable_income = Decimal(line11_agi) - line14
        if line15_taxable_income < 0: line15_taxable_income = Decimal('0.0') 
        _set_field_value(populated_structure, "Line15_TaxableIncome", float(line15_taxable_income))
        logger.info(f"Recalculated 1040 Line 15 (Taxable Income) using Sch A: {line15_taxable_income:.2f}")

        # --- Recalculate tax and subsequent lines ---
        taxable_income = line15_taxable_income # Use the recalculated taxable income
        line16_tax = Decimal('0.0')
        if filing_status == 'Single':
             if taxable_income <= 11000: line16_tax = taxable_income * Decimal('0.10')
             elif taxable_income <= 44725: line16_tax = 1100 + (taxable_income - 11000) * Decimal('0.12')
             elif taxable_income <= 95375: line16_tax = 5147 + (taxable_income - 44725) * Decimal('0.22')
             else: line16_tax = 16271.75 + (taxable_income - 95375) * Decimal('0.24')
        elif filing_status == 'MarriedFilingJointly':
             if taxable_income <= 22000: line16_tax = taxable_income * Decimal('0.10')
             elif taxable_income <= 89450: line16_tax = 2200 + (taxable_income - 22000) * Decimal('0.12')
             elif taxable_income <= 190750: line16_tax = 10294 + (taxable_income - 89450) * Decimal('0.22')
             else: line16_tax = 32580 + (taxable_income - 190750) * Decimal('0.24')
        else: 
             if taxable_income <= 11000: line16_tax = taxable_income * Decimal('0.10')
             elif taxable_income <= 44725: line16_tax = 1100 + (taxable_income - 11000) * Decimal('0.12')
             elif taxable_income <= 95375: line16_tax = 5147 + (taxable_income - 44725) * Decimal('0.22')
             else: line16_tax = 16271.75 + (taxable_income - 95375) * Decimal('0.24')
             logger.warning(f"Unknown or unhandled filing status '{filing_status}'. Using simplified Single tax brackets.")
        line16_tax = line16_tax.quantize(Decimal('0.01'))
        _set_field_value(populated_structure, "Line16_Tax", float(line16_tax))
        logger.info(f"Recalculated 1040 Line 16 (Tax) using Sch A/brackets: {line16_tax:.2f}")
        
        # --- Recalculate remaining lines using updated Line 16 tax ---
        line17 = _get_numeric_value(populated_structure, "Line17_AmountFromSchedule2") 
        line18 = line16_tax + Decimal(line17)
        _set_field_value(populated_structure, "Line18_TotalTaxBeforeCredits", float(line18))

        line21 = _get_numeric_value(populated_structure, "Line21_TotalNonrefundableCredits") 
        line22 = line18 - Decimal(line21)
        if line22 < 0: line22 = Decimal('0.0')
        _set_field_value(populated_structure, "Line22_TaxAfterNonrefundableCredits", float(line22))

        line23 = _get_numeric_value(populated_structure, "Line23_OtherTaxes") 
        line24_total_tax = line22 + Decimal(line23)
        _set_field_value(populated_structure, "Line24_TotalTax", float(line24_total_tax))
        logger.info(f"Recalculated 1040 Line 24 (Total Tax) using Sch A: {line24_total_tax:.2f}")
        
        line33_total_payments = _get_numeric_value(populated_structure, "Line33_TotalPayments") 
        line34_overpaid = Decimal('0.0')
        if Decimal(line33_total_payments) > line24_total_tax:
            line34_overpaid = Decimal(line33_total_payments) - line24_total_tax
        _set_field_value(populated_structure, "Line34_AmountOverpaid", float(line34_overpaid))
        logger.info(f"Recalculated 1040 Line 34 (Overpaid) using Sch A: {line34_overpaid:.2f}")

        line37_amount_owed = Decimal('0.0')
        if line24_total_tax > Decimal(line33_total_payments):
             line37_amount_owed = line24_total_tax - Decimal(line33_total_payments)
        _set_field_value(populated_structure, "Line37_AmountYouOwe", float(line37_amount_owed))
        logger.info(f"Recalculated 1040 Line 37 (Amount Owed) using Sch A: {line37_amount_owed:.2f}")

    elif form_type == 'SchedC':
        # --- Schedule C Calculations ---
        logger.info("Performing Schedule C calculations...")

        # Line 3 = Line 1 - Line 2
        line1 = _get_numeric_value(populated_structure, "Line1_GrossReceiptsSales")
        line2 = _get_numeric_value(populated_structure, "Line2_ReturnsAllowances")
        line3 = line1 - line2
        _set_field_value(populated_structure, "Line3_GrossProfit", line3)
        logger.info(f"Calculated SchedC Line 3 (Gross Profit): {line3}")

        # Line 5 = Line 3 - Line 4
        line4 = _get_numeric_value(populated_structure, "Line4_CostOfGoodsSold")
        line5 = line3 - line4
        _set_field_value(populated_structure, "Line5_GrossIncome", line5)
        logger.info(f"Calculated SchedC Line 5 (Gross Income): {line5}")

        # Line 28 = Sum of lines 8 through 27a
        line8 = _get_numeric_value(populated_structure, "Line8_Advertising")
        line9 = _get_numeric_value(populated_structure, "Line9_CarTruckExpenses")
        line10 = _get_numeric_value(populated_structure, "Line10_CommissionsFees")
        line11 = _get_numeric_value(populated_structure, "Line11_ContractLabor")
        line12 = _get_numeric_value(populated_structure, "Line12_Depletion")
        line13 = _get_numeric_value(populated_structure, "Line13_DepreciationSection179")
        line14 = _get_numeric_value(populated_structure, "Line14_EmployeeBenefitPrograms")
        line15 = _get_numeric_value(populated_structure, "Line15_Insurance")
        line16a = _get_numeric_value(populated_structure, "Line16a_InterestMortgage")
        line16b = _get_numeric_value(populated_structure, "Line16b_InterestOther")
        line17 = _get_numeric_value(populated_structure, "Line17_LegalProfessionalServices")
        line18 = _get_numeric_value(populated_structure, "Line18_OfficeExpense")
        line19 = _get_numeric_value(populated_structure, "Line19_PensionProfitSharing")
        line20a = _get_numeric_value(populated_structure, "Line20a_RentLeaseVehicles")
        line20b = _get_numeric_value(populated_structure, "Line20b_RentLeaseOther")
        line21 = _get_numeric_value(populated_structure, "Line21_RepairsMaintenance")
        line22 = _get_numeric_value(populated_structure, "Line22_Supplies")
        line23 = _get_numeric_value(populated_structure, "Line23_TaxesLicenses")
        line24a = _get_numeric_value(populated_structure, "Line24a_Travel")
        line24b = _get_numeric_value(populated_structure, "Line24b_DeductibleMeals")
        line25 = _get_numeric_value(populated_structure, "Line25_Utilities")
        line26 = _get_numeric_value(populated_structure, "Line26_Wages")
        line27a = _get_numeric_value(populated_structure, "Line27a_OtherExpenses")

        line28 = sum([
            line8, line9, line10, line11, line12, line13, line14, line15, 
            line16a, line16b, line17, line18, line19, line20a, line20b, 
            line21, line22, line23, line24a, line24b, line25, line26, line27a
        ])
        _set_field_value(populated_structure, "Line28_TotalExpenses", line28)
        logger.info(f"Calculated SchedC Line 28 (Total Expenses): {line28}")

        # Line 31 = Line 5 - Line 28
        line31 = line5 - line28
        _set_field_value(populated_structure, "Line31_NetProfitLoss", line31)
        logger.info(f"Calculated SchedC Line 31 (Net Profit/Loss): {line31}")
        
        # Placeholder for other Sched C calculations (e.g., Line 30 - Business Use of Home from Form 8829)

    elif form_type == '1040-SE':
        # ... (1040-SE calculation logic) ...
        pass
    elif form_type == 'SchedE':
        # --- Schedule E Calculations ---
        logger.info("Performing Schedule E calculations...")
        total_income_loss_line26 = Decimal('0.0')
        property_columns = ['A', 'B', 'C']

        for col in property_columns:
            # Check if this property column has any data (e.g., Rent Received)
            rent_field = f"SchedE_Line3_RentsReceived{col}"
            if _get_field_value(populated_structure, rent_field) is None: # Skip if no rent for this property
                continue 
                
            logger.info(f"Calculating totals for Sched E Column {col}...")
            # Line 20: Sum expenses for this property
            line5 = _get_numeric_value(populated_structure, f"SchedE_Line5_Advertising{col}")
            line6 = _get_numeric_value(populated_structure, f"SchedE_Line6_AutoTravel{col}")
            line7 = _get_numeric_value(populated_structure, f"SchedE_Line7_CleaningMaintenance{col}")
            line8 = _get_numeric_value(populated_structure, f"SchedE_Line8_Commissions{col}")
            line9 = _get_numeric_value(populated_structure, f"SchedE_Line9_Insurance{col}")
            line10 = _get_numeric_value(populated_structure, f"SchedE_Line10_LegalProfessionalFees{col}")
            line11 = _get_numeric_value(populated_structure, f"SchedE_Line11_ManagementFees{col}")
            line12 = _get_numeric_value(populated_structure, f"SchedE_Line12_MortgageInterestBanks{col}")
            line13 = _get_numeric_value(populated_structure, f"SchedE_Line13_OtherInterest{col}")
            line14 = _get_numeric_value(populated_structure, f"SchedE_Line14_Repairs{col}")
            line15 = _get_numeric_value(populated_structure, f"SchedE_Line15_Supplies{col}")
            line16 = _get_numeric_value(populated_structure, f"SchedE_Line16_Taxes{col}")
            line17 = _get_numeric_value(populated_structure, f"SchedE_Line17_Utilities{col}")
            line18 = _get_numeric_value(populated_structure, f"SchedE_Line18_DepreciationDepletion{col}")
            line19 = _get_numeric_value(populated_structure, f"SchedE_Line19_OtherExpense{col}")
            
            line20_total_expenses = sum([
                line5, line6, line7, line8, line9, line10, line11, line12, 
                line13, line14, line15, line16, line17, line18, line19
            ])
            _set_field_value(populated_structure, f"SchedE_Line20_TotalExpenses{col}", line20_total_expenses)
            logger.info(f"Calculated SchedE Line 20 (Total Expenses {col}): {line20_total_expenses}")

            # Line 21: Income or Loss = Line 3 + Line 4 - Line 20
            line3_rents = _get_numeric_value(populated_structure, rent_field)
            line4_royalties = _get_numeric_value(populated_structure, f"SchedE_Line4_RoyaltiesReceived{col}")
            line21_income_loss = (Decimal(line3_rents) + Decimal(line4_royalties)) - Decimal(line20_total_expenses)
            _set_field_value(populated_structure, f"SchedE_Line21_IncomeLoss{col}", float(line21_income_loss))
            logger.info(f"Calculated SchedE Line 21 (Income/Loss {col}): {line21_income_loss:.2f}")

            # Line 22: Deductible loss (Ignoring PAL for now, assume Line 21 if loss)
            line22_deductible_loss = Decimal('0.0')
            if line21_income_loss < 0:
                 line22_deductible_loss = line21_income_loss # Simple version
            _set_field_value(populated_structure, f"SchedE_Line22_DeductibleLoss{col}", float(line22_deductible_loss))
            
            # Accumulate total for Line 26 (Ignoring PAL)
            # TODO: Implement Passive Activity Loss (PAL) limitations using Form 8582 logic
            total_income_loss_line26 += line21_income_loss
            
        # Line 26: Total supplemental income or (loss) - Part I only for now
        # TODO: Add results from Parts II-V when implemented
        _set_field_value(populated_structure, "SchedE_Line26_TotalIncomeLoss", float(total_income_loss_line26))
        logger.info(f"Calculated SchedE Line 26 (Total Income/Loss - Part I only): {total_income_loss_line26:.2f}")
        
        # Note: Line 26 transfers to Schedule 1, Line 5

    elif form_type == 'Schedule 1':
        # ... (Schedule 1 calculation logic) ...
        pass
    elif form_type == 'Schedule 2':
        # ... (Schedule 2 calculation logic) ...
        pass
    elif form_type == 'Schedule 3':
        # ... (Schedule 3 calculation logic) ...
        pass
    elif form_type == 'Form 2441':
        # ... (Form 2441 calculation logic) ...
        pass
    elif form_type == 'Form 8812':
        # ... (Form 8812 calculation logic) ...
        pass
    elif form_type == 'Schedule A':
        # --- Schedule A Calculations ---
        logger.info("Performing Schedule A calculations...")

        # Get AGI from Form 1040 Cache (Needed for limitations)
        form1040_structure = populated_structures_cache.get('1040', {})
        agi = _get_numeric_value(form1040_structure, "Line11_AdjustedGrossIncome")
        _set_field_value(populated_structure, "SchA_Line2_AGI", agi)
        logger.info(f"Fetched AGI for Sch A: {agi:.2f}")
        
        # --- Medical and Dental Expenses ---
        line1_medical_raw = _get_numeric_value(populated_structure, "SchA_Line1_MedicalDentalExpenses")
        line3_agi_limit_medical = agi * Decimal('0.075')
        _set_field_value(populated_structure, "SchA_Line3_AGILimit", float(line3_agi_limit_medical))
        line4_deductible_medical = max(Decimal(0), Decimal(line1_medical_raw) - line3_agi_limit_medical)
        _set_field_value(populated_structure, "SchA_Line4_DeductibleMedical", float(line4_deductible_medical))
        logger.info(f"Calculated SchA Line 4 (Deductible Medical): {line4_deductible_medical:.2f}")
        
        # --- Taxes You Paid ---
        line5a_salt = _get_numeric_value(populated_structure, "SchA_Line5a_StateLocalTaxes")
        line5b_real_estate = _get_numeric_value(populated_structure, "SchA_Line5b_RealEstateTaxes")
        line5c_personal_prop = _get_numeric_value(populated_structure, "SchA_Line5c_PersonalPropertyTaxes")
        line5d_total_salt_prop = sum([Decimal(line5a_salt), Decimal(line5b_real_estate), Decimal(line5c_personal_prop)])
        _set_field_value(populated_structure, "SchA_Line5d_TotalSALT", float(line5d_total_salt_prop))
        filing_status = _get_field_value(form1040_structure, "FilingStatus")
        salt_cap = Decimal(5000) if filing_status == 'MarriedFilingSeparately' else Decimal(10000)
        line5e_capped_salt = min(line5d_total_salt_prop, salt_cap)
        _set_field_value(populated_structure, "SchA_Line5e_LimitedSALT", float(line5e_capped_salt))
        logger.info(f"Calculated SchA Line 5e (Limited SALT): {line5e_capped_salt:.2f}")
        line6_other_taxes = _get_numeric_value(populated_structure, "SchA_Line6_OtherTaxes")
        line7_total_taxes = line5e_capped_salt + Decimal(line6_other_taxes)
        _set_field_value(populated_structure, "SchA_Line7_TotalTaxes", float(line7_total_taxes))
        logger.info(f"Calculated SchA Line 7 (Total Taxes): {line7_total_taxes:.2f}")
        
        # --- Interest You Paid ---
        line8a = _get_numeric_value(populated_structure, "SchA_Line8a_HomeMortgageInterest")
        line8e_total_home_interest = Decimal(line8a)
        _set_field_value(populated_structure, "SchA_Line8e_TotalHomeMortgageInterest", float(line8e_total_home_interest))
        line9_investment_interest = Decimal('0.0')
        _set_field_value(populated_structure, "SchA_Line9_InvestmentInterest", float(line9_investment_interest))
        line10_total_interest = line8e_total_home_interest + line9_investment_interest
        _set_field_value(populated_structure, "SchA_Line10_TotalInterest", float(line10_total_interest))
        logger.info(f"Calculated/Placeholder SchA Line 10 (Total Interest): {line10_total_interest:.2f}")
        
        # --- Gifts to Charity ---
        line11_cash = _get_numeric_value(populated_structure, "SchA_Line11_ContributionsCash")
        line12_noncash = _get_numeric_value(populated_structure, "SchA_Line12_ContributionsOther")
        line13_carryover = _get_numeric_value(populated_structure, "SchA_Line13_Carryover")
        line14_total_charity = sum([Decimal(line11_cash), Decimal(line12_noncash), Decimal(line13_carryover)])
        _set_field_value(populated_structure, "SchA_Line14_TotalContributions", float(line14_total_charity))
        logger.info(f"Calculated/Placeholder SchA Line 14 (Total Charity - No AGI Limit): {line14_total_charity:.2f}")

        # --- Casualty and Theft Losses (Line 15) ---
        line15_casualty_loss = Decimal('0.0')
        _set_field_value(populated_structure, "SchA_Line15_CasualtyTheftLoss", float(line15_casualty_loss))

        # --- Other Itemized Deductions (Line 16) ---
        line16_other_deductions = Decimal('0.0')
        _set_field_value(populated_structure, "SchA_Line16_OtherDeductions", float(line16_other_deductions))
        
        # --- Total Itemized Deductions (Line 17) ---
        line17_total_itemized = sum([
            line4_deductible_medical, line7_total_taxes, line10_total_interest, 
            line14_total_charity, line15_casualty_loss, line16_other_deductions
        ])
        _set_field_value(populated_structure, "SchA_Line17_TotalItemizedDeductions", float(line17_total_itemized))
        logger.info(f"Calculated SchA Line 17 (Total Itemized Deductions): {line17_total_itemized:.2f}")
        # Note: Line 17 is compared to standard deduction on Form 1040, Line 12

    else:
        # This block should ideally not be reached if all expected forms have specific calculation blocks.
        # If reached, it means calculations weren't defined for this form_type.
        logger.warning(f"No specific calculation logic implemented for form_type: {form_type}. Skipping calculations.")
        
    # Return the structure, potentially modified by the calculations above.
    logger.info(f"Calculations complete for {form_type}.")
    return populated_structure # Return the structure that was passed in and potentially modified

# --- Prefect Task (Refactored) --- 
@task
def create_populated_gemini_structure(
    aggregated_data_by_type: Dict[str, List[Dict[str, Any]]], 
    target_form: str, 
    gemini_blank_fields_path: str,
    populated_structures_cache: Dict[str, Any] # Add cache parameter
) -> Dict[str, Any]:
    """ 
    Creates a populated version of the Gemini blank fields structure.
    Takes aggregated data grouped by type (with source info), performs form-specific aggregation,
    maps aggregated values (with sources) to Gemini field names, injects them into the blank structure,
    and performs calculations based on IRS rules.
    """
    # --- Start of Function Body ---
    print(f"Creating populated Gemini fields structure for: {target_form}")
    logger = get_run_logger()

    # 1. Load the blank Gemini fields structure (as before)
    try:
        with open(gemini_blank_fields_path, 'r') as f:
            gemini_structure = json.load(f)
        logger.info(f"Successfully loaded blank Gemini fields structure from {gemini_blank_fields_path}")
    except Exception as e:
        logger.error(f"Failed to load blank Gemini fields from {gemini_blank_fields_path}: {e}")
        return {"error": f"Failed to load blank structure: {e}"}

    # 2. Perform Form-Specific Aggregation (including source tracking)
    final_aggregated_values = {} # Stores {"value": agg_value, "sources": [...] or list of these
    # Define keys here or pass them to the helper?
    relevant_doc_types = []
    proprietor_keys = {"EmployeeName", "EmployeeSSN"}
    summable_keys = set()
    modal_keys = set()
    list_keys = set()

    # --- Define Aggregation Rules per Form ---
    if target_form == '1040':
        relevant_doc_types = ["W-2"] # Add others like 1099-INT, DIV etc.
        summable_keys = {"WagesTipsOtherComp", "FederalIncomeTaxWithheld", "StateWagesTipsEtc", "StateIncomeTax", "TotalTaxableInterest", "TotalOrdinaryDividends", "TotalTaxableIRADistributions", "TotalTaxablePensionsAnnuities", "TotalTaxableSocialSecurity", "Aggregated1099Withholding", "AggregatedOtherWithholding", "EstimatedTaxPaymentsMade"}
        modal_keys = {"EmployeeName", "EmployeeSSN", "TaxYear", "EmployeeAddress", "FilingStatus"}
        list_keys = {"EmployerName", "EmployerEIN", "DependentName", "DependentSSN", "DependentRelationship"}
        # Call the new aggregation helper
        final_aggregated_values = _aggregate_data_for_form(
            aggregated_data_by_type, relevant_doc_types, summable_keys, modal_keys, list_keys, proprietor_keys
        )

    elif target_form == 'SchedC':
        relevant_doc_types = ["Profit and Loss Statement", "1099-NEC", "Invoice", "Receipt", "Insurance Policy"]
        summable_keys = { "TotalRevenue", "NonemployeeCompensation", "GrossReceiptsOrSales", "ReturnsAllowances", "CostOfGoodsSold", "AdvertisingExpense", "CarTruckExpense", "CommissionsFeesExpense", "ContractLaborExpense", "DepletionExpense", "DepreciationExpense", "EmployeeBenefitExpense", "InsuranceExpense", "InterestMortgageExpense", "InterestOtherExpense", "LegalProfessionalExpense", "OfficeExpense", "PensionProfitSharingExpense", "RentLeaseVehicleExpense", "RentLeaseOtherExpense", "RepairsMaintenanceExpense", "SuppliesExpense", "TaxesLicensesExpense", "TravelExpense", "MealsExpense", "UtilitiesExpense", "WagesExpense", "OtherExpenseAmount"}
        modal_keys = {"BusinessName", "PrincipalBusinessActivity", "EIN", "BusinessAddress", "BusinessCityStateZip"}
        list_keys = {"OtherExpenseDescription"}
        # DEBUG: Log relevant input data for Sched C aggregation
        logger.debug(f"SchedC Aggregation Input Data ({relevant_doc_types}): { {k: v for k, v in aggregated_data_by_type.items() if k in relevant_doc_types} }")
        # Call the new aggregation helper
        final_aggregated_values = _aggregate_data_for_form(
            aggregated_data_by_type, relevant_doc_types, summable_keys, modal_keys, list_keys, proprietor_keys
        )
        # DEBUG: Log aggregated results for Sched C
        logger.debug(f"SchedC Final Aggregated Values: {final_aggregated_values}")

    elif target_form == '1040-SE':
         relevant_doc_types = ["Profit and Loss Statement", "W-2"]
         summable_keys = {"NetIncomeLoss", "WagesTipsOtherComp", "SocialSecurityWages"}
         modal_keys = {"EmployeeName", "EmployeeSSN"}
         list_keys = {}
         # Call the new aggregation helper
         final_aggregated_values = _aggregate_data_for_form(
             aggregated_data_by_type, relevant_doc_types, summable_keys, modal_keys, list_keys, proprietor_keys
         )

    elif target_form == 'SchedE':
         # --- Schedule E Specific Aggregation --- 
         logger.info(f"Performing Schedule E specific aggregation...")
         final_aggregated_values_sched_e = {}
         # DEBUG: Log relevant input data for Sched E aggregation
         sched_e_relevant_types = ["Cash Flow Statement", "Profit and Loss Statement"]
         logger.debug(f"SchedE Aggregation Input Data ({sched_e_relevant_types}): { {k: v for k, v in aggregated_data_by_type.items() if k in sched_e_relevant_types} }")

         prop_col_map = {} # Map property address/ID to column letter (A, B, C)
         next_col_idx = 0
         col_letters = ['A', 'B', 'C']
         relevant_doc_types = ["Cash Flow Statement", "Profit and Loss Statement"]
         income_keys = {"TotalRevenue", "RentalIncome"}
         expense_map = { 
             "AdvertisingExpense": "SchedE_Line5_Advertising", "AutoTravelExpense": "SchedE_Line6_AutoTravel",
             "CleaningMaintenanceExpense": "SchedE_Line7_CleaningMaintenance", "CommissionsExpense": "SchedE_Line8_Commissions",
             "InsuranceExpense": "SchedE_Line9_Insurance", "LegalProfessionalExpense": "SchedE_Line10_LegalProfessionalFees",
             "ManagementFeeExpense": "SchedE_Line11_ManagementFees", "MortgageInterestExpense": "SchedE_Line12_MortgageInterestBanks",
             "OtherInterestExpense": "SchedE_Line13_OtherInterest", "RepairsExpense": "SchedE_Line14_Repairs",
             "SuppliesExpense": "SchedE_Line15_Supplies", "TaxesExpense": "SchedE_Line16_Taxes",
             "UtilitiesExpense": "SchedE_Line17_Utilities", "DepreciationExpense": "SchedE_Line18_DepreciationDepletion",
             "TotalOperatingExpenses": "SchedE_Line19_OtherExpenseA"
         }
         summable_keys = income_keys.union(set(expense_map.keys()))
         for doc_type in relevant_doc_types:
             if doc_type in aggregated_data_by_type:
                 for document_data in aggregated_data_by_type[doc_type]:
                     prop_id = None
                     prop_addr_data = document_data.get("PropertyAddress")
                     if prop_addr_data and isinstance(prop_addr_data, dict) and prop_addr_data.get("value"):
                         prop_id = _clean_string(prop_addr_data["value"])
                     else:
                         source = document_data.get("PropertyAddress", {}).get("source", "UnknownSource")
                         prop_id = source 
                         logger.warning(f"Could not find PropertyAddress in {source}, using source as identifier: {prop_id}")
                     if prop_id not in prop_col_map:
                         if next_col_idx < len(col_letters):
                             col = col_letters[next_col_idx]
                             prop_col_map[prop_id] = col
                             final_aggregated_values_sched_e[col] = {}
                             logger.info(f"Assigning property '{prop_id}' to Schedule E column {col}")
                             next_col_idx += 1
                         else:
                             logger.warning(f"Found more than 3 properties for Schedule E. Skipping property: {prop_id}")
                             continue
                     col = prop_col_map[prop_id]
                     prop_data = final_aggregated_values_sched_e[col]
                     for key, value_source_dict in document_data.items():
                         value = value_source_dict.get('value')
                         source = value_source_dict.get('source')
                         cleaned_value = _clean_string(value)
                         if cleaned_value is None or source is None: continue
                         target_agg_key = None
                         if key in income_keys: target_agg_key = "SchedE_Line3_RentsReceived"
                         elif key in expense_map: target_agg_key = expense_map[key]
                         if target_agg_key:
                             agg_info = prop_data.setdefault(target_agg_key, {"total": 0.0, "sources": set()})
                             try:
                                 numeric_value = float(re.sub(r'[$,()]', '', cleaned_value))
                                 agg_info["total"] += numeric_value
                                 agg_info["sources"].add(source)
                             except (ValueError, TypeError):
                                 logger.warning(f"Could not convert '{cleaned_value}' to number for Sched E key '{key}'. Skipping value.")
         for col_data in final_aggregated_values_sched_e.values():
             for key, agg_info in col_data.items():
                 col_data[key] = {"value": agg_info["total"], "sources": sorted(list(agg_info["sources"]))}
         final_aggregated_values = final_aggregated_values_sched_e
         # --- End Schedule E Specific Aggregation ---
         # DEBUG: Log aggregated results for Sched E
         logger.debug(f"SchedE Final Aggregated Values: {final_aggregated_values}")

    elif target_form == 'Schedule 1':
        relevant_doc_types = ['Alimony Agreement', 'Unemployment Statement', 'Gambling Winnings Form', 'Student Loan Interest Statement', 'HSA Contribution Form', 'IRA Contribution Form'] 
        summable_keys = {'AlimonyReceived', 'TaxableRefundsCreditsOffsets', 'UnemploymentCompensation','OtherIncomeAmount', 'EducatorExpenses', 'HSA_DeductionAmount', 'MovingExpensesAmount', 'SE_HealthInsuranceDeductionAmount', 'SEP_SIMPLE_QualifiedPlanDeduction','AlimonyPaid', 'IRA_DeductionAmount', 'StudentLoanInterestDeduction'}
        modal_keys = {'AlimonyRecipientSSN'}
        list_keys = {'OtherIncomeDescription'} 
        # Call the new aggregation helper
        final_aggregated_values = _aggregate_data_for_form(
            aggregated_data_by_type, relevant_doc_types, summable_keys, modal_keys, list_keys, proprietor_keys
        )

    elif target_form == 'Schedule 2':
        relevant_doc_types = ['Form 6251 Data', 'Form 8962 Data'] 
        summable_keys = {'AlternativeMinimumTaxAmount', 'ExcessAdvancePTCRepaymentAmount'}
        modal_keys = {}
        list_keys = {}
        # Call the new aggregation helper
        final_aggregated_values = _aggregate_data_for_form(
            aggregated_data_by_type, relevant_doc_types, summable_keys, modal_keys, list_keys, proprietor_keys
        )

    elif target_form == 'Schedule 3':
        relevant_doc_types = ['Form 4868', 'W-2', 'Form 1099', 'Form 2439'] 
        summable_keys = {'AmountPaidWithExtension', 'ExcessSocialSecurityTaxWithheld', 'CreditFromForm2439'}
        modal_keys = {}
        list_keys = {}
        # Call the new aggregation helper
        final_aggregated_values = _aggregate_data_for_form(
            aggregated_data_by_type, relevant_doc_types, summable_keys, modal_keys, list_keys, proprietor_keys
        )

    elif target_form == 'Form 2441':
        relevant_doc_types = ['Child Care Statement', 'W-2'] 
        summable_keys = {'ChildCareExpenses', 'EmployerProvidedDependentCareBenefits'}
        modal_keys = {'DependentCareProviderName', 'ProviderAddress', 'ProviderTaxID'} 
        list_keys = {'DependentNameForCare', 'DependentSSNForCare'}
        # Call the new aggregation helper
        final_aggregated_values = _aggregate_data_for_form(
            aggregated_data_by_type, relevant_doc_types, summable_keys, modal_keys, list_keys, proprietor_keys
        )

    elif target_form == 'Form 8812':
        # Form 8812 relies heavily on dependents and AGI from 1040.
        # We might need to aggregate dependent info specifically if not already done.
        relevant_doc_types = ["W-2"] # Need W-2 for dependents primarily
        summable_keys = {}
        modal_keys = {}
        # Aggregate Dependent info using list_keys
        list_keys = {"DependentName", "DependentSSN", "DependentRelationship"}
        # Call the aggregation helper specifically for dependents
        final_aggregated_values = _aggregate_data_for_form(
            aggregated_data_by_type, relevant_doc_types, summable_keys, modal_keys, list_keys, proprietor_keys
        )
        # Log the result for debugging
        if 'DependentName' in final_aggregated_values:
             logger.info(f"Aggregated dependent info for Form 8812: {len(final_aggregated_values['DependentName'])} dependents.")
        else:
             logger.warning("Could not aggregate dependent info for Form 8812.")

    elif target_form == 'Schedule A':
        relevant_doc_types = ['Medical Bill', 'Property Tax Statement', 'Mortgage Interest Statement (1098)', 'Charitable Donation Receipt']
        summable_keys = {'MedicalExpenses', 'StateAndLocalTaxes', 'RealEstateTaxes', 'PersonalPropertyTaxes', 'HomeMortgageInterest', 'InvestmentInterest', 'CharitableContributionsCash', 'CharitableContributionsNonCash', 'CasualtyTheftLossAmount', 'OtherItemizedDeductionAmount'}
        modal_keys = {}
        list_keys = {'OtherItemizedDeductionDescription'}
        # Call the new aggregation helper
        final_aggregated_values = _aggregate_data_for_form(
            aggregated_data_by_type, relevant_doc_types, summable_keys, modal_keys, list_keys, proprietor_keys
        )

    else:
        # --- Default Aggregation Logic (if no specific block matched) ---
        logger.info(f"Using default aggregation logic for {target_form} (no specific rules defined).")
        relevant_doc_types = list(aggregated_data_by_type.keys())
        # Define generic key sets or leave them empty for minimal aggregation
        summable_keys = set() # Or try to infer?
        modal_keys = set()
        list_keys = set()
        proprietor_keys = {"EmployeeName", "EmployeeSSN"}
        # Call the new aggregation helper
        final_aggregated_values = _aggregate_data_for_form(
            aggregated_data_by_type, relevant_doc_types, summable_keys, modal_keys, list_keys, proprietor_keys
        )

    # --- Mapping & Injection --- 
    logger.info(f"Form-specific aggregation complete. Result keys: {list(final_aggregated_values.keys())}")

    # 3. Map Aggregated Values to Target Gemini Field Names
    mapped_gemini_data = {}
    if target_form != 'SchedE': # Schedule E mapping is handled dynamically during injection
        mapped_gemini_data = _map_aggregated_to_gemini_fields(final_aggregated_values, target_form)
        # DEBUG: Log mapping results for non-SchedE forms
        if target_form == 'SchedC': # Add specific logging for Sched C mapping output
             logger.debug(f"SchedC Mapped Gemini Data: {mapped_gemini_data}")
        if not mapped_gemini_data:
             logger.warning(f"Mapping step produced no data for form {target_form}.")
             # Don't return early, still might have calculations to perform

    # 4. Inject mapped values AND sources into the loaded Gemini structure
    populated_count = 0
    for page_key, page_content in gemini_structure.items():
        if isinstance(page_content, dict) and "fields" in page_content and isinstance(page_content["fields"], list):
            for field_def in page_content["fields"]:
                if isinstance(field_def, dict) and "field_name" in field_def:
                    gemini_field_name = field_def["field_name"]
                    aggregated_info = None
                    if target_form == 'SchedE':
                        # Dynamic Sched E Injection (Map and Inject directly)
                        # Determine column based on field name (e.g., '...A', '...B', '...C')
                        col_match = re.search(r'([ABC])$', gemini_field_name)
                        if col_match:
                            col = col_match.group(1)
                            # Map the generic Sched E field name back to an aggregation key
                            base_field_name = gemini_field_name[:-1] # Remove trailing A/B/C
                            
                            # Check if this base name corresponds to an aggregated key for this column
                            # Need to reverse the logic slightly from aggregation
                            # Example: If gemini_field_name is SchedE_Line9_InsuranceA
                            # We look for SchedE_Line9_Insurance in final_aggregated_values['A']
                            agg_key_to_find = base_field_name # Assuming agg keys match base field names now
                            
                            if col in final_aggregated_values and agg_key_to_find in final_aggregated_values[col]:
                                aggregated_info = final_aggregated_values[col][agg_key_to_find]
                                # DEBUG: Log Sched E injection mapping
                                # logger.debug(f"SchedE Inject: Found {agg_key_to_find} in col {col} for {gemini_field_name}")
                        else:
                             # Handle fields without A/B/C suffix if any (e.g., SchedE_Line26_TotalIncomeLoss)
                             # This might need direct mapping if not handled by column logic
                             if gemini_field_name in final_aggregated_values: # Check top level
                                 aggregated_info = final_aggregated_values[gemini_field_name]
                    else: # Use pre-mapped data for other forms
                        if gemini_field_name in mapped_gemini_data:
                            aggregated_info = mapped_gemini_data[gemini_field_name]
                    # Inject if we found aggregated info
                    if aggregated_info is not None:
                        if isinstance(aggregated_info, dict):
                            field_def["value"] = aggregated_info.get("value")
                            field_def["sources"] = aggregated_info.get("sources")
                        elif isinstance(aggregated_info, list):
                            field_def["value"] = aggregated_info 
                        else:
                            field_def["value"] = aggregated_info 
                        logger.info(f"Injected value/sources for {gemini_field_name}")
                        # DEBUG: Log the actual injected value for Sched C/E fields
                        if target_form in ['SchedC', 'SchedE']:
                             logger.debug(f"Injected into {target_form} field '{gemini_field_name}': Value = {field_def.get('value')}")
                        populated_count += 1
        else:
             logger.warning(f"Unexpected structure for page key: {page_key}")
    logger.info(f"Injection complete. Populated {populated_count} fields in the structure.")

    # 5. Perform Calculations
    populated_structure_with_calcs = _perform_calculations(
        gemini_structure, # Pass structure post-injection
        target_form, 
        populated_structures_cache,
        final_aggregated_values # Pass aggregated data needed by calculations (e.g., dependents)
    )

    # 6. Return the modified structure (now with calculated fields)
    return populated_structure_with_calcs

# ... (Rest of file) ...