"""
Task for validating populated form data and aggregating confidence scores.
"""

from typing import Dict, Any, List, Tuple, Optional
from prefect import task
import statistics

# Assume validation rule modules are loaded via helpers
# Assume schema is available

def _run_validation_checks(schema_compliant_json: Dict[str, Any], rules_module: Any) -> Dict[str, List[Dict[str, str]]]:
    """Runs validation checks defined in the rules module."""
    if not rules_module or not hasattr(rules_module, 'run_all_validations'):
        print("Warning: Validation rules module is invalid or missing 'run_all_validations' function. Skipping checks.")
        return {"errors": [], "warnings": []}
    
    try:
        errors, warnings = rules_module.run_all_validations(schema_compliant_json)
        print(f"Validation checks ran: {len(errors)} errors, {len(warnings)} warnings found.")
        return {"errors": errors, "warnings": warnings}
    except Exception as e:
        print(f"Error running validation checks: {e}")
        return {"errors": [{ "error": f"Validation engine failed: {e}" }], "warnings": []}

def _calculate_aggregated_confidence(schema_compliant_json: Dict[str, Any]) -> Dict[str, Any]:
    """Calculates field-level and overall confidence scores."""
    confidences = schema_compliant_json.get("_confidence_scores", {}) # Get confidence dict added during mapping
    field_confidences = {}
    all_scores = []

    for key, score_list in confidences.items():
        if score_list:
            avg_score = statistics.mean(score_list)
            field_confidences[key] = round(avg_score, 3)
            all_scores.extend(score_list)
        else:
            field_confidences[key] = None # No score available
    
    overall_confidence = None
    if all_scores:
        overall_confidence = round(statistics.mean(all_scores), 3)
    
    return {
        "field_level": field_confidences,
        "overall": overall_confidence
    }

def _determine_review_need(validation_results: Dict[str, List], aggregated_confidence: Dict[str, Any], confidence_threshold: float = 0.85) -> bool:
    """Determines if human review is needed based on validation and confidence."""
    if validation_results.get('errors'):
        print("Flagging for review due to validation errors.")
        return True
    
    overall_confidence = aggregated_confidence.get('overall')
    if overall_confidence is not None and overall_confidence < confidence_threshold:
        print(f"Flagging for review due to low overall confidence ({overall_confidence} < {confidence_threshold}).")
        return True

    # Check field-level confidence (optional - could have different thresholds)
    # low_conf_fields = {k: v for k, v in aggregated_confidence.get('field_level', {}).items() if v is not None and v < confidence_threshold}
    # if low_conf_fields:
    #     print(f"Flagging for review due to low confidence fields: {low_conf_fields}")
    #     return True

    # Add checks for specific critical validation warnings if needed

    return False

@task
def validate_form(schema_compliant_json: Dict[str, Any], filled_pdf_path: str, target_form: str, validation_rules: Dict[str, Any]) -> Dict[str, Any]:
    """Prefect task to perform validation checks and aggregate confidence."""
    print(f"Starting validation for form: {target_form} ({filled_pdf_path})" )

    rules_module = validation_rules.get(target_form)

    # Perform consistency checks, calculations, IRS rule checks
    validation_results = _run_validation_checks(schema_compliant_json, rules_module)

    # Aggregate confidence scores from IE/Mapping stages
    aggregated_confidence = _calculate_aggregated_confidence(schema_compliant_json)

    # Determine human review flags based on validation results and confidence
    # TODO: Make threshold configurable
    needs_review = _determine_review_need(validation_results, aggregated_confidence, confidence_threshold=0.80)

    validation_report = {
        "status": "Validated" if not validation_results['errors'] else "Errors Found",
        "errors": validation_results.get('errors', []), 
        "warnings": validation_results.get('warnings', []), 
        "confidence": aggregated_confidence,
        "needs_human_review": needs_review,
        "target_form": target_form,
        "filled_pdf_path": filled_pdf_path # Include path for reference
    }
    print(f"Validation complete for: {target_form}. Needs Review: {needs_review}")
    return validation_report 