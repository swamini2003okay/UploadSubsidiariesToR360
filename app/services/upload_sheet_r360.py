import os
import re
from dotenv import load_dotenv
from app.services.cosmos import CosmosService
from loguru import logger

load_dotenv()

def get_parent_id_by_psd(psd_number: str) -> str:
    """
    Fetch the main parent ID from Cosmos DB using a PSD number.
    
    Args:
        psd_number (str): The PSD number to search for
        
    Returns:
        str: The main parent ID if found, None otherwise
    """
    try:
        # Initialize Cosmos service
        cosmos_service = CosmosService()
        
        # Query to find document with the given PSD number
        query = """
        SELECT c.main_parent_id 
        FROM c 
        WHERE c.dType = @d_type 
        AND c.psd_number = @psd_number 
        AND c.active = @is_active
        """
        
        parameters = [
            {"name": "@d_type", "value": "main_parent"},
            {"name": "@psd_number", "value": psd_number},
            {"name": "@is_active", "value": True}
        ]
        
        # Execute the query
        items = list(cosmos_service.container.query_items(
            query=query,
            parameters=parameters,
            enable_cross_partition_query=True
        ))
        
        if not items:
            logger.warning(f"No document found with PSD number: {psd_number}")
            return None
            
        # Assuming the first match is what we want
        parent_id = items[0].get('main_parent_id')
        
        if not parent_id:
            logger.warning(f"Document found but no parentId for PSD: {psd_number}")
            return None
            
        logger.info(f"Found parent ID {parent_id} for PSD: {psd_number}")
        return parent_id
        
    except Exception as e:
        logger.error(f"Error fetching parent ID for PSD {psd_number}: {str(e)}")
        raise

# Need to change this function as we need to fetch the psd from s3 so need to use result from get_sheet_from_s3
def get_psd_by_sheet(filename: str) -> str:
    """
    Extract the PSD number from a filename in the format:
    'LegalEntityMapping_SFDC-PSD-{number}_{timestamp}'
    
    Args:
        filename (str): The input filename (can be with or without path)
        
    Returns:
        str: The PSD number in format 'SFDC-PSD-{number}'
        
    Raises:
        ValueError: If the filename doesn't match the expected pattern
    """
    # Match the pattern: LegalEntityMapping_SFDC-PSD-{numbers}_{numbers}
    match = re.match(r'^LegalEntityMapping_(SFDC-PSD-\d+)_\d+', filename)
    
    if not match:
        raise ValueError(f"Filename '{filename}' does not match expected pattern: 'LegalEntityMapping_SFDC-PSD-{{number}}_{{timestamp}}'")
    
    return match.group(1)


if __name__ == "__main__":
    # Example usage
    psd = input("Enter PSD number: ").strip()
    if psd:
        parent_id = get_parent_id_by_psd(psd)
        if parent_id:
            print(f"Main Parent ID: {parent_id}")
        else:
            print("No matching record found for the given PSD number.")
    else:
        print("No PSD number provided.")
        
    # Example usage of get_psd_sheet
    test_filename = "LegalEntityMapping_SFDC-PSD-076858_1767096012389.csv"
    try:
        psd_number = get_psd_by_sheet(test_filename)
        print(f"\nExtracted PSD number from '{test_filename}': {psd_number}")
    except ValueError as e:
        print(f"\nError: {str(e)}")