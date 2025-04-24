import json
import os
import fitz

def load_schemas():
    with open('schemas/1040.json', 'r') as f:
        schema_1040 = json.load(f)
    
    with open('schemas/SchedC.json', 'r') as f:
        schema_schedC = json.load(f)
    
    return schema_1040, schema_schedC

def flatten_schema(schema, prefix=''):
    """Flatten a nested JSON schema into field keys with dot notation"""
    flattened = {}
    
    if not isinstance(schema, dict):
        return flattened
    
    # Handle special case for definitions
    if 'definitions' in schema:
        definitions = schema.pop('definitions', {})
        # Process the rest of the schema first
    
    # Process schema properties directly
    properties = schema.get('properties', {})
    for key, value in properties.items():
        new_prefix = f"{prefix}.{key}" if prefix else key
        
        if isinstance(value, dict) and 'properties' in value:
            # This is a nested object schema
            nested_flat = flatten_schema(value, new_prefix)
            flattened.update(nested_flat)
        elif isinstance(value, dict) and '$ref' in value:
            # Handle references (basic implementation)
            ref_path = value['$ref']
            if ref_path.startswith('#/definitions/'):
                definition_key = ref_path.split('/')[-1]
                if definitions and definition_key in definitions:
                    # Recursively process the definition
                    nested_flat = flatten_schema(definitions[definition_key], new_prefix)
                    flattened.update(nested_flat)
        elif isinstance(value, dict) and 'type' in value:
            # This is a leaf property
            flattened[new_prefix] = value
    
    return flattened

def get_pdf_fields(pdf_path):
    """Extract all field names from a PDF"""
    doc = fitz.open(pdf_path)
    field_info = {}
    page_to_field = {}
    
    for page_num, page in enumerate(doc):
        page_fields = []
        widget = page.first_widget
        while widget:
            field_name = widget.field_name
            field_type = widget.field_type
            
            # Determine field type name
            if field_type == fitz.PDF_WIDGET_TYPE_TEXT:
                field_type_name = "text"
            elif field_type == fitz.PDF_WIDGET_TYPE_CHECKBOX:
                field_type_name = "checkbox"
            elif field_type == fitz.PDF_WIDGET_TYPE_RADIOBUTTON:
                field_type_name = "radiobutton"
            else:
                field_type_name = "other"
            
            # Get field position for helping with identification
            rect = widget.rect
            field_info[field_name] = {
                "type": field_type_name,
                "page": page_num + 1,
                "rect": [round(v, 2) for v in rect]
            }
            page_fields.append(field_name)
            widget = widget.next
        
        page_to_field[page_num + 1] = page_fields
    
    doc.close()
    return field_info, page_to_field

def create_mapping():
    """Create field mappings for both forms"""
    # 1. Load schemas
    schema_1040, schema_schedC = load_schemas()
    
    # 2. Flatten schemas
    flat_1040 = flatten_schema(schema_1040)
    flat_schedC = flatten_schema(schema_schedC)
    
    # 3. Get PDF field information
    f1040_fields, f1040_pages = get_pdf_fields('templates/f1040_blank.pdf')
    schedC_fields, schedC_pages = get_pdf_fields('templates/f1040sc_blank.pdf')
    
    # Print some summary info
    print(f"Form 1040 Schema: {len(flat_1040)} fields")
    print(f"Form 1040 PDF: {len(f1040_fields)} fields")
    print(f"Schedule C Schema: {len(flat_schedC)} fields")
    print(f"Schedule C PDF: {len(schedC_fields)} fields")
    
    # 4. Create initial mappings based on manual inspection and analysis
    # Format: schema_field_name -> pdf_field_name
    
    # Form 1040 Mapping - use the flattened format (with _ instead of dots)
    f1040_mapping = {
        # Personal Info - using actual field names found in the PDF
        "personal_info_taxpayer_name": "topmostSubform[0].Page1[0].f1_01[0]",
        "personal_info_spouse_name": "topmostSubform[0].Page1[0].f1_02[0]",
        "personal_info_taxpayer_ssn": "topmostSubform[0].Page1[0].f1_03[0]",
        "personal_info_spouse_ssn": "topmostSubform[0].Page1[0].f1_04[0]",
        "personal_info_address": "topmostSubform[0].Page1[0].Address_ReadOrder[0].f1_10[0]",
        
        # Filing Status - checkboxes
        "personal_info_filing_status": "topmostSubform[0].Page1[0].FilingStatus_ReadOrder[0].c1_3[0]",
        
        # Digital Assets Question
        "digital_assets_question": "topmostSubform[0].Page1[0].c1_20[0]",
        
        # Income
        "income_line1z_wages": "topmostSubform[0].Page1[0].Line1-8c_ReadOrder[0].f1_25[0]",
        "income_line2a_tax_exempt_interest": "topmostSubform[0].Page1[0].Line1-8c_ReadOrder[0].f1_27[0]",
        "income_line2b_taxable_interest": "topmostSubform[0].Page1[0].Line1-8c_ReadOrder[0].f1_29[0]",
        "income_line3a_qualified_dividends": "topmostSubform[0].Page1[0].Line1-8c_ReadOrder[0].f1_31[0]",
        "income_line3b_ordinary_dividends": "topmostSubform[0].Page1[0].Line1-8c_ReadOrder[0].f1_33[0]",
        "income_line4b_ira_taxable": "topmostSubform[0].Page1[0].Line4a-11_ReadOrder[0].f1_48[0]",
        "income_line5b_pensions_taxable": "topmostSubform[0].Page1[0].Line4a-11_ReadOrder[0].f1_51[0]",
        "income_line6b_ss_benefits_taxable": "topmostSubform[0].Page1[0].Line4a-11_ReadOrder[0].f1_57[0]",
        "income_line7_capital_gain_loss": "topmostSubform[0].Page1[0].Line4a-11_ReadOrder[0].f1_58[0]",
        "income_line8_schedule1_line10_income": "topmostSubform[0].Page1[0].Line4a-11_ReadOrder[0].f1_59[0]",
        "income_line9_total_income": "topmostSubform[0].Page1[0].Line4a-11_ReadOrder[0].f1_60[0]",
        
        # Adjustments
        "adjustments_line10_schedule1_line26_adjustments": "topmostSubform[0].Page1[0].Line4a-11_ReadOrder[0].f1_61[0]",
        "adjustments_line11_adjusted_gross_income": "topmostSubform[0].Page1[0].Line4a-11_ReadOrder[0].f1_62[0]"
    }
    
    # Schedule C Mapping - use the flattened format (with _ instead of dots)
    schedC_mapping = {
        # Business Information
        "business_info_proprietor_name": "topmostSubform[0].Page1[0].NameAndSocial_ReadOrder[0].f1_1[0]",
        "business_info_proprietor_ssn": "topmostSubform[0].Page1[0].NameAndSocial_ReadOrder[0].f1_2[0]",
        "business_info_business_name": "topmostSubform[0].Page1[0].NameAndSocial_ReadOrder[0].f1_3[0]",
        "business_info_business_code": "topmostSubform[0].Page1[0].f1_4[0]",
        "business_info_ein": "topmostSubform[0].Page1[0].f1_5[0]",
        "business_info_business_address": "topmostSubform[0].Page1[0].DComb[0].f1_6[0]",
        
        # Accounting Method - checkboxes
        "business_info_accounting_method": "topmostSubform[0].Page1[0].c1_1[0]",  # Cash method
        
        # Material Participation
        "business_info_material_participation": "topmostSubform[0].Page1[0].c1_2[0]",
        
        # Form 1099 Filing Req - checkboxes
        "business_info_form_1099_filing_req": "topmostSubform[0].Page1[0].c1_3[0]",
        
        # Income
        "income_line1_gross_receipts": "topmostSubform[0].Page1[0].Lines1-7[0].f1_9[0]",
        "income_line2_returns_allowances": "topmostSubform[0].Page1[0].Lines1-7[0].f1_10[0]",
        "income_line4_cogs": "topmostSubform[0].Page1[0].Lines1-7[0].f1_12[0]",
        "income_line6_other_income": "topmostSubform[0].Page1[0].Lines1-7[0].f1_14[0]",
        "income_line7_gross_income": "topmostSubform[0].Page1[0].Lines1-7[0].f1_15[0]",
        
        # Expenses
        "expenses_advertising": "topmostSubform[0].Page1[0].Lines8-17[0].f1_17[0]",
        "expenses_car_truck_expenses": "topmostSubform[0].Page1[0].Lines8-17[0].f1_18[0]",
        "expenses_commissions_fees": "topmostSubform[0].Page1[0].Lines8-17[0].f1_19[0]",
        "expenses_contract_labor": "topmostSubform[0].Page1[0].Lines8-17[0].f1_20[0]",
        "expenses_depletion": "topmostSubform[0].Page1[0].Lines8-17[0].f1_21[0]",
        "expenses_depreciation_sec179": "topmostSubform[0].Page1[0].Lines8-17[0].f1_22[0]",
        "expenses_employee_benefits": "topmostSubform[0].Page1[0].Lines8-17[0].f1_23[0]",
        "expenses_insurance_other": "topmostSubform[0].Page1[0].Lines8-17[0].f1_24[0]",
        "expenses_interest_mortgage": "topmostSubform[0].Page1[0].Lines8-17[0].f1_25[0]",
        "expenses_interest_other": "topmostSubform[0].Page1[0].Lines8-17[0].f1_26[0]",
        "expenses_legal_professional": "topmostSubform[0].Page1[0].Lines8-17[0].f1_27[0]",
        "expenses_office_expense": "topmostSubform[0].Page1[0].Lines18-27[0].f1_28[0]",
        "expenses_pension_profit_sharing": "topmostSubform[0].Page1[0].Lines18-27[0].f1_29[0]",
        "expenses_rent_lease_vehicle": "topmostSubform[0].Page1[0].Lines18-27[0].f1_30[0]",
        "expenses_rent_lease_other": "topmostSubform[0].Page1[0].Lines18-27[0].f1_31[0]",
        "expenses_repairs_maintenance": "topmostSubform[0].Page1[0].Lines18-27[0].f1_32[0]",
        "expenses_supplies": "topmostSubform[0].Page1[0].Lines18-27[0].f1_33[0]",
        "expenses_taxes_licenses": "topmostSubform[0].Page1[0].Lines18-27[0].f1_34[0]",
        "expenses_travel": "topmostSubform[0].Page1[0].Lines18-27[0].f1_35[0]",
        "expenses_meals": "topmostSubform[0].Page1[0].Lines18-27[0].f1_36[0]",
        "expenses_utilities": "topmostSubform[0].Page1[0].Lines18-27[0].f1_37[0]",
        "expenses_wages": "topmostSubform[0].Page1[0].Lines18-27[0].f1_38[0]",
        
        # Net Profit/Loss
        "net_profit_loss_line28_total_expenses_before_home": "topmostSubform[0].Page1[0].Lines18-27[0].f1_41[0]",
        "net_profit_loss_line29_tentative_profit_loss": "topmostSubform[0].Page1[0].Line30_ReadOrder[0].f1_42[0]",
        "net_profit_loss_line30_business_use_home": "topmostSubform[0].Page1[0].Line30_ReadOrder[0].f1_43[0]",
        "net_profit_loss_line31_net_profit_loss": "topmostSubform[0].Page1[0].Line30_ReadOrder[0].f1_44[0]",
        "net_profit_loss_line32a_at_risk_loss": "topmostSubform[0].Page1[0].c1_4[0]",
    }
    
    # 5. Save the mappings to JSON files
    os.makedirs('mappings', exist_ok=True)
    
    with open('mappings/1040_field_mapping.json', 'w') as f:
        json.dump(f1040_mapping, f, indent=2)
    
    with open('mappings/schedC_field_mapping.json', 'w') as f:
        json.dump(schedC_mapping, f, indent=2)
    
    print(f"Created Form 1040 mapping with {len(f1040_mapping)} fields")
    print(f"Created Schedule C mapping with {len(schedC_mapping)} fields")
    
    # 6. Identify unmapped schema fields (for reference)
    unmapped_1040 = [k for k in flat_1040.keys() if k not in f1040_mapping]
    unmapped_schedC = [k for k in flat_schedC.keys() if k not in schedC_mapping]
    
    print(f"\nUnmapped 1040 schema fields: {len(unmapped_1040)}")
    print(f"Unmapped Schedule C schema fields: {len(unmapped_schedC)}")
    
    # 7. Identify unmapped PDF fields (for reference)
    mapped_1040_pdf_fields = set(f1040_mapping.values())
    mapped_schedC_pdf_fields = set(schedC_mapping.values())
    
    unmapped_1040_pdf = [k for k in f1040_fields.keys() if k not in mapped_1040_pdf_fields]
    unmapped_schedC_pdf = [k for k in schedC_fields.keys() if k not in mapped_schedC_pdf_fields]
    
    print(f"Unmapped 1040 PDF fields: {len(unmapped_1040_pdf)}")
    print(f"Unmapped Schedule C PDF fields: {len(unmapped_schedC_pdf)}")
    
    return f1040_mapping, schedC_mapping

if __name__ == "__main__":
    create_mapping()