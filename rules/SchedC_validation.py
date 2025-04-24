"""
Placeholder for Schedule C validation rules.
"""
from typing import Dict, Any, List, Tuple

def check_gross_income_calculation(form_data: Dict[str, Any]) -> Dict[str, str]:
    """Example validation: Check Line 7 (Gross Income) calculation."""
    # Line 7 = Line 1 (Gross receipts) - Line 2 (Returns) + Line 4 (COGS) + Line 6 (Other income)
    # Note: COGS (Line 4) is subtracted from Gross Receipts (Line 3) before adding Other Income (Line 6)
    # So, Gross Income (Line 7) = Line 3 + Line 6. Line 3 = Line 1 - Line 2.
    # Let's verify Line 7 = (Line 1 - Line 2 - Line 4) + Line 6 is incorrect based on form. (Typo in my initial logic)
    # Correct: Line 7 = Line 1 - Line 2 - Line 4 + Line 6 ? No. Line 5 = Line 3 - Line 4. Line 7 = Line 5 + Line 6.
    # Let's trace: Line 3 = Line 1 - Line 2. Line 5 = Line 3 - Line 4. Line 7 = Line 5 + Line 6.
    # Simplified: Line 7 = (Line 1 - Line 2) - Line 4 + Line 6.

    line1 = form_data.get('income', {}).get('line1_gross_receipts', 0) or 0
    line2 = form_data.get('income', {}).get('line2_returns_allowances', 0) or 0
    # Line 4 (COGS) should come from the COGS section after calculation
    line4 = form_data.get('cost_of_goods_sold', {}).get('cogs_total', 0) or 0
    line6 = form_data.get('income', {}).get('line6_other_income', 0) or 0

    calculated_gross_income = (line1 - line2) - line4 + line6 # Based on Line 7 = Line 5 + Line 6, where Line 5 = (Line 1-Line 2) - Line 4

    reported_gross_income = form_data.get('income', {}).get('line7_gross_income')

    if reported_gross_income is None:
        return {"warning": "Line 7 (Gross Income) is missing."}

    if abs(calculated_gross_income - (reported_gross_income or 0)) > 0.01:
        return {"error": f"Line 7 (Gross Income) calculation mismatch. Calculated: {calculated_gross_income:.2f}, Reported: {reported_gross_income}"}
    return {}

def check_total_expenses_calculation(form_data: Dict[str, Any]) -> Dict[str, str]:
    """Example validation: Check Line 28 (Total Expenses before home use) calculation."""
    # Sum lines 8 through 27b
    expenses = form_data.get('expenses', {})
    calculated_total = sum(v for k, v in expenses.items() if v is not None)

    reported_total = form_data.get('net_profit_loss', {}).get('line28_total_expenses_before_home')

    if reported_total is None:
        return {"warning": "Line 28 (Total Expenses) is missing."}

    if abs(calculated_total - (reported_total or 0)) > 0.01:
        return {"error": f"Line 28 (Total Expenses) calculation mismatch. Calculated: {calculated_total:.2f}, Reported: {reported_total}"}
    return {}

def check_net_profit_loss_calculation(form_data: Dict[str, Any]) -> Dict[str, str]:
    """Example validation: Check Line 31 (Net Profit/Loss) calculation."""
    # Line 31 = Line 29 (Tentative Profit = Line 7 - Line 28) - Line 30 (Business Use of Home)
    line7 = form_data.get('income', {}).get('line7_gross_income', 0) or 0
    line28 = form_data.get('net_profit_loss', {}).get('line28_total_expenses_before_home', 0) or 0
    line30 = form_data.get('net_profit_loss', {}).get('line30_business_use_home', 0) or 0

    calculated_net_profit = (line7 - line28) - line30

    reported_net_profit = form_data.get('net_profit_loss', {}).get('line31_net_profit_loss')

    if reported_net_profit is None:
        return {"warning": "Line 31 (Net Profit/Loss) is missing."}

    if abs(calculated_net_profit - (reported_net_profit or 0)) > 0.01:
        return {"error": f"Line 31 (Net Profit/Loss) calculation mismatch. Calculated: {calculated_net_profit:.2f}, Reported: {reported_net_profit}"}
    return {}


# Add more validation functions here for:
# - COGS calculations (Part III)
# - Consistency between Part IV (Vehicle) and Line 9
# - Material participation rules
# - At-risk limitations (Line 32)
# - Consistency of accounting method

ALL_RULES = [
    check_gross_income_calculation,
    check_total_expenses_calculation,
    check_net_profit_loss_calculation,
    # Add other rule functions here
]

def run_all_validations(form_data: Dict[str, Any]) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    """Runs all defined validation rules for Schedule C."""
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