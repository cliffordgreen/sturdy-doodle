"""
Placeholder for Form 1040 validation rules.
Contains functions to perform consistency checks, calculations, and IRS rule checks.
"""
from typing import Dict, Any, List, Tuple

def check_total_income(form_data: Dict[str, Any]) -> Dict[str, str]:
    """Example validation: Check if Line 9 (Total Income) matches calculated sum."""
    calculated_total = (
        form_data.get('income', {}).get('line1z_wages', 0) or 0) + \
        (form_data.get('income', {}).get('line2b_taxable_interest', 0) or 0) + \
        (form_data.get('income', {}).get('line3b_ordinary_dividends', 0) or 0) + \
        (form_data.get('income', {}).get('line4b_ira_taxable', 0) or 0) + \
        (form_data.get('income', {}).get('line5b_pensions_taxable', 0) or 0) + \
        (form_data.get('income', {}).get('line6b_ss_benefits_taxable', 0) or 0) + \
        (form_data.get('income', {}).get('line7_capital_gain_loss', 0) or 0) + \
        (form_data.get('income', {}).get('line8_schedule1_line10_income', 0) or 0
    )
    reported_total = form_data.get('income', {}).get('line9_total_income')

    if reported_total is None:
        return {"warning": "Line 9 (Total Income) is missing."}

    # Use a small tolerance for floating point comparisons
    if abs(calculated_total - (reported_total or 0)) > 0.01:
        return {"error": f"Line 9 (Total Income) calculation mismatch. Calculated: {calculated_total:.2f}, Reported: {reported_total}"}
    return {}

def check_schedule_b_requirement(form_data: Dict[str, Any]) -> Dict[str, str]:
    """Example validation: Check if Schedule B might be required."""
    taxable_interest = form_data.get('income', {}).get('line2b_taxable_interest', 0) or 0
    ordinary_dividends = form_data.get('income', {}).get('line3b_ordinary_dividends', 0) or 0
    # Assume schedule_b_attached is a field populated if Sch B was processed
    schedule_b_attached = form_data.get('internal_metadata', {}).get('schedule_b_attached', False)

    if (taxable_interest > 1500 or ordinary_dividends > 1500) and not schedule_b_attached:
         return {"warning": "Schedule B may be required for taxable interest or ordinary dividends > $1500."}
    return {}

# Add more validation functions here for:
# - AGI calculation
# - Taxable Income calculation
# - Deduction limitations
# - Credit calculations (EIC, Child Tax Credit, etc.)
# - Filing status consistency
# - Dependent eligibility checks
# - Cross-form consistency (e.g., values from Sch 1)
# - Completeness checks (mandatory fields)

ALL_RULES = [
    check_total_income,
    check_schedule_b_requirement,
    # Add other rule functions here
]

def run_all_validations(form_data: Dict[str, Any]) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    """Runs all defined validation rules for Form 1040."""
    errors = []
    warnings = []
    for rule_func in ALL_RULES:
        result = rule_func(form_data)
        if result:
            if 'error' in result:
                errors.append(result)
            elif 'warning' in result:
                warnings.append(result)
    return errors, warnings 