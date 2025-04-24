import json
import os
import fitz

def load_form_analysis(analysis_dir, form_name):
    """Load the detailed PDF form analysis"""
    analysis_file = os.path.join(analysis_dir, f"{form_name}_detailed_analysis.json")
    vertical_map_file = os.path.join(analysis_dir, f"{form_name}_vertical_map.json")
    
    if not os.path.exists(analysis_file) or not os.path.exists(vertical_map_file):
        print(f"Analysis files not found for {form_name}")
        return None, None
    
    with open(analysis_file, 'r') as f:
        analysis = json.load(f)
    
    with open(vertical_map_file, 'r') as f:
        vertical_map = json.load(f)
    
    return analysis, vertical_map

def load_schema(schema_file):
    """Load a form schema"""
    with open(schema_file, 'r') as f:
        schema = json.load(f)
    return schema

def flatten_schema(schema, prefix=''):
    """Flatten a nested schema into keys with underscore notation"""
    flattened = {}
    
    def _process_properties(props, current_prefix):
        for key, value in props.items():
            new_prefix = f"{current_prefix}_{key}" if current_prefix else key
            
            if 'properties' in value:
                # This is a nested object
                _process_properties(value['properties'], new_prefix)
            elif '$ref' in value:
                # This is a reference to another schema part
                ref_path = value['$ref']
                if ref_path.startswith('#/definitions/'):
                    ref_key = ref_path.split('/')[-1]
                    if 'definitions' in schema and ref_key in schema['definitions']:
                        ref_def = schema['definitions'][ref_key]
                        if 'properties' in ref_def:
                            _process_properties(ref_def['properties'], new_prefix)
            else:
                # This is a leaf property
                flattened[new_prefix] = value
    
    if 'properties' in schema:
        _process_properties(schema['properties'], prefix)
    
    return flattened

def create_form_1040_mapping(analysis, vertical_map, schema):
    """Create a comprehensive mapping for Form 1040"""
    field_mapping = {}
    
    # Flatten the schema
    flat_schema = flatten_schema(schema)
    print(f"Flattened schema contains {len(flat_schema)} fields")
    
    # Create a lookup by field position on the form
    field_positions = {}
    for page_key, page_data in analysis['pages'].items():
        for field in page_data['fields']:
            field_positions[field['name']] = {
                'position': field['position'],
                'type': field['type']
            }
    
    # Map personal information fields (first section of 1040)
    personal_info_mappings = {
        'personal_info_taxpayer_name': ('topmostSubform[0].Page1[0].f1_01[0]', 'text'),
        'personal_info_spouse_name': ('topmostSubform[0].Page1[0].f1_02[0]', 'text'),
        'personal_info_taxpayer_ssn': ('topmostSubform[0].Page1[0].f1_03[0]', 'text'),
        'personal_info_spouse_ssn': ('topmostSubform[0].Page1[0].f1_04[0]', 'text'),
        'personal_info_address': ('topmostSubform[0].Page1[0].Address_ReadOrder[0].f1_10[0]', 'text'),
        'personal_info_filing_status': ('topmostSubform[0].Page1[0].FilingStatus_ReadOrder[0].c1_1[0]', 'checkbox'),
    }
    field_mapping.update(personal_info_mappings)
    
    # Map income fields
    income_mappings = {
        'income_line1z_wages': ('topmostSubform[0].Page1[0].Line1-8c_ReadOrder[0].f1_25[0]', 'text'),
        'income_line2a_tax_exempt_interest': ('topmostSubform[0].Page1[0].Line1-8c_ReadOrder[0].f1_27[0]', 'text'),
        'income_line2b_taxable_interest': ('topmostSubform[0].Page1[0].Line1-8c_ReadOrder[0].f1_29[0]', 'text'),
        'income_line3a_qualified_dividends': ('topmostSubform[0].Page1[0].Line1-8c_ReadOrder[0].f1_31[0]', 'text'),
        'income_line3b_ordinary_dividends': ('topmostSubform[0].Page1[0].Line1-8c_ReadOrder[0].f1_33[0]', 'text'),
        'income_line4b_ira_taxable': ('topmostSubform[0].Page1[0].Line4a-11_ReadOrder[0].f1_48[0]', 'text'),
        'income_line5b_pensions_taxable': ('topmostSubform[0].Page1[0].Line4a-11_ReadOrder[0].f1_51[0]', 'text'),
        'income_line6b_ss_benefits_taxable': ('topmostSubform[0].Page1[0].Line4a-11_ReadOrder[0].f1_57[0]', 'text'),
        'income_line7_capital_gain_loss': ('topmostSubform[0].Page1[0].f1_65[0]', 'text'),
        'income_line8_schedule1_line10_income': ('topmostSubform[0].Page1[0].f1_66[0]', 'text'),
        'income_line9_total_income': ('topmostSubform[0].Page1[0].f1_67[0]', 'text'),
    }
    field_mapping.update(income_mappings)
    
    # Map adjustment fields
    adjustment_mappings = {
        'adjustments_line10_schedule1_line26_adjustments': ('topmostSubform[0].Page1[0].f1_68[0]', 'text'),
        'adjustments_line11_adjusted_gross_income': ('topmostSubform[0].Page1[0].f1_69[0]', 'text'),
    }
    field_mapping.update(adjustment_mappings)
    
    # Digital assets question
    field_mapping['digital_assets_question'] = ('topmostSubform[0].Page1[0].c1_20[0]', 'checkbox')
    
    # Create final mapping dictionary (schema_key -> pdf_field_name)
    final_mapping = {}
    for schema_key, (pdf_field, field_type) in field_mapping.items():
        final_mapping[schema_key] = pdf_field
    
    print(f"Created 1040 mapping with {len(final_mapping)} fields")
    return final_mapping

def create_schedule_c_mapping(analysis, vertical_map, schema):
    """Create a comprehensive mapping for Schedule C"""
    field_mapping = {}
    
    # Flatten the schema
    flat_schema = flatten_schema(schema)
    print(f"Flattened schema contains {len(flat_schema)} fields")
    
    # Map business information
    business_info_mappings = {
        'business_info_proprietor_name': ('topmostSubform[0].Page1[0].NameAndSocial_ReadOrder[0].f1_1[0]', 'text'),
        'business_info_proprietor_ssn': ('topmostSubform[0].Page1[0].NameAndSocial_ReadOrder[0].f1_2[0]', 'text'),
        'business_info_business_name': ('topmostSubform[0].Page1[0].NameAndSocial_ReadOrder[0].f1_3[0]', 'text'),
        'business_info_business_code': ('topmostSubform[0].Page1[0].f1_4[0]', 'text'),
        'business_info_ein': ('topmostSubform[0].Page1[0].f1_5[0]', 'text'),
        'business_info_business_address': ('topmostSubform[0].Page1[0].DComb[0].f1_6[0]', 'text'),
        'business_info_accounting_method': ('topmostSubform[0].Page1[0].c1_1[0]', 'checkbox'),
        'business_info_material_participation': ('topmostSubform[0].Page1[0].c1_2[0]', 'checkbox'),
        'business_info_form_1099_filing_req': ('topmostSubform[0].Page1[0].c1_3[0]', 'checkbox'),
    }
    field_mapping.update(business_info_mappings)
    
    # Map income
    income_mappings = {
        'income_line1_gross_receipts': ('topmostSubform[0].Page1[0].Lines1-7[0].f1_9[0]', 'text'),
        'income_line2_returns_allowances': ('topmostSubform[0].Page1[0].Lines1-7[0].f1_10[0]', 'text'),
        'income_line4_cogs': ('topmostSubform[0].Page1[0].Lines1-7[0].f1_12[0]', 'text'),
        'income_line6_other_income': ('topmostSubform[0].Page1[0].Lines1-7[0].f1_14[0]', 'text'),
        'income_line7_gross_income': ('topmostSubform[0].Page1[0].Lines1-7[0].f1_15[0]', 'text'),
    }
    field_mapping.update(income_mappings)
    
    # Map expenses
    expense_mappings = {
        'expenses_advertising': ('topmostSubform[0].Page1[0].Lines8-17[0].f1_17[0]', 'text'),
        'expenses_car_truck_expenses': ('topmostSubform[0].Page1[0].Lines8-17[0].f1_18[0]', 'text'),
        'expenses_commissions_fees': ('topmostSubform[0].Page1[0].Lines8-17[0].f1_19[0]', 'text'),
        'expenses_contract_labor': ('topmostSubform[0].Page1[0].Lines8-17[0].f1_20[0]', 'text'),
        'expenses_depletion': ('topmostSubform[0].Page1[0].Lines8-17[0].f1_21[0]', 'text'),
        'expenses_depreciation_sec179': ('topmostSubform[0].Page1[0].Lines8-17[0].f1_22[0]', 'text'),
        'expenses_employee_benefits': ('topmostSubform[0].Page1[0].Lines8-17[0].f1_23[0]', 'text'),
        'expenses_insurance_other': ('topmostSubform[0].Page1[0].Lines8-17[0].f1_24[0]', 'text'),
        'expenses_interest_mortgage': ('topmostSubform[0].Page1[0].Lines8-17[0].f1_25[0]', 'text'),
        'expenses_interest_other': ('topmostSubform[0].Page1[0].Lines8-17[0].f1_26[0]', 'text'),
        'expenses_legal_professional': ('topmostSubform[0].Page1[0].Lines8-17[0].f1_27[0]', 'text'),
        'expenses_office_expense': ('topmostSubform[0].Page1[0].Lines18-27[0].f1_28[0]', 'text'),
        'expenses_pension_profit_sharing': ('topmostSubform[0].Page1[0].Lines18-27[0].f1_29[0]', 'text'),
        'expenses_rent_lease_vehicle': ('topmostSubform[0].Page1[0].Lines18-27[0].f1_30[0]', 'text'),
        'expenses_rent_lease_other': ('topmostSubform[0].Page1[0].Lines18-27[0].f1_31[0]', 'text'),
        'expenses_repairs_maintenance': ('topmostSubform[0].Page1[0].Lines18-27[0].f1_32[0]', 'text'),
        'expenses_supplies': ('topmostSubform[0].Page1[0].Lines18-27[0].f1_33[0]', 'text'),
        'expenses_taxes_licenses': ('topmostSubform[0].Page1[0].Lines18-27[0].f1_34[0]', 'text'),
        'expenses_travel': ('topmostSubform[0].Page1[0].Lines18-27[0].f1_35[0]', 'text'),
        'expenses_meals': ('topmostSubform[0].Page1[0].Lines18-27[0].f1_36[0]', 'text'),
        'expenses_utilities': ('topmostSubform[0].Page1[0].Lines18-27[0].f1_37[0]', 'text'),
        'expenses_wages': ('topmostSubform[0].Page1[0].Lines18-27[0].f1_38[0]', 'text'),
    }
    field_mapping.update(expense_mappings)
    
    # Map net profit/loss fields
    profit_loss_mappings = {
        'net_profit_loss_line28_total_expenses_before_home': ('topmostSubform[0].Page1[0].Lines18-27[0].f1_41[0]', 'text'),
        'net_profit_loss_line29_tentative_profit_loss': ('topmostSubform[0].Page1[0].Line30_ReadOrder[0].f1_42[0]', 'text'),
        'net_profit_loss_line30_business_use_home': ('topmostSubform[0].Page1[0].Line30_ReadOrder[0].f1_43[0]', 'text'),
        'net_profit_loss_line31_net_profit_loss': ('topmostSubform[0].Page1[0].Line30_ReadOrder[0].f1_44[0]', 'text'),
        'net_profit_loss_line32a_at_risk_loss': ('topmostSubform[0].Page1[0].c1_4[0]', 'checkbox'),
    }
    field_mapping.update(profit_loss_mappings)
    
    # Create final mapping dictionary (schema_key -> pdf_field_name)
    final_mapping = {}
    for schema_key, (pdf_field, field_type) in field_mapping.items():
        final_mapping[schema_key] = pdf_field
    
    print(f"Created Schedule C mapping with {len(final_mapping)} fields")
    return final_mapping

def save_mapping(mapping, output_file):
    """Save the mapping to a JSON file"""
    with open(output_file, 'w') as f:
        json.dump(mapping, f, indent=2)
    print(f"Saved mapping to {output_file}")

def print_mapping_stats(mapping, pdf_field_count, schema_field_count=None):
    """Print statistics about the mapping coverage"""
    mapped_fields = set(mapping.values())
    if schema_field_count:
        schema_coverage = len(mapping) / schema_field_count * 100
        print(f"Schema coverage: {len(mapping)}/{schema_field_count} fields ({schema_coverage:.1f}%)")
    
    pdf_coverage = len(mapped_fields) / pdf_field_count * 100
    print(f"PDF form coverage: {len(mapped_fields)}/{pdf_field_count} fields ({pdf_coverage:.1f}%)")

def main():
    # Create output directory
    os.makedirs('mappings', exist_ok=True)
    
    # Process Form 1040
    print("\n=== Processing Form 1040 ===")
    f1040_analysis, f1040_vmap = load_form_analysis('analysis', 'f1040_blank')
    f1040_schema = load_schema('schemas/1040.json')
    f1040_mapping = create_form_1040_mapping(f1040_analysis, f1040_vmap, f1040_schema)
    save_mapping(f1040_mapping, 'mappings/1040_field_mapping.json')
    flat_1040_schema = flatten_schema(f1040_schema)
    print_mapping_stats(f1040_mapping, f1040_analysis['total_fields'], len(flat_1040_schema))
    
    # Process Schedule C
    print("\n=== Processing Schedule C ===")
    schedC_analysis, schedC_vmap = load_form_analysis('analysis', 'f1040sc_blank')
    schedC_schema = load_schema('schemas/SchedC.json')
    schedC_mapping = create_schedule_c_mapping(schedC_analysis, schedC_vmap, schedC_schema)
    save_mapping(schedC_mapping, 'mappings/schedC_field_mapping.json')
    flat_schedC_schema = flatten_schema(schedC_schema)
    print_mapping_stats(schedC_mapping, schedC_analysis['total_fields'], len(flat_schedC_schema))

if __name__ == "__main__":
    main()