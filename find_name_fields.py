import fitz

def find_name_fields(pdf_path):
    print(f"Looking for name fields in: {pdf_path}")
    doc = fitz.open(pdf_path)
    field_list = []
    
    for page_num, page in enumerate(doc):
        widget = page.first_widget
        while widget:
            field_list.append((widget.field_name, widget.field_type, page_num+1))
            widget = widget.next
    
    print(f"Found {len(field_list)} total fields")
    
    name_fields = [f for f in field_list if 'name' in f[0].lower()]
    if name_fields:
        print("Name-related fields:")
        for field_name, field_type, page_num in name_fields:
            print(f"  Page {page_num}: {field_name} (type: {field_type})")
    else:
        print("No name-related fields found")
        
    first_fields = [f for f in field_list if 'f1_' in f[0]]
    if first_fields:
        print("\nFirst 10 form fields with 'f1_' prefix:")
        for field_name, field_type, page_num in first_fields[:10]:
            print(f"  Page {page_num}: {field_name} (type: {field_type})")
    
    doc.close()

if __name__ == "__main__":
    find_name_fields("templates/f1040_blank.pdf")