from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from typing import Optional
import logging
from app.services.upload_sheet_r360 import get_psd_by_sheet, get_parent_id_by_psd

# Create router
router = APIRouter()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@router.get("/psd", response_model=dict, summary="Extract PSD from filename")
async def get_psd(
    filename: str = Query(
        ...,
        description="Filename in format 'LegalEntityMapping_SFDC-PSD-{number}_{timestamp}'",
        examples={"example1": {"value": "LegalEntityMapping_SFDC-PSD-076858_1767096012389"}}
    )
):
    """
    Extract PSD number from a properly formatted filename.
    
    The filename should be in the format: LegalEntityMapping_SFDC-PSD-{number}_{timestamp}
    """
    try:
        psd_number = get_psd_by_sheet(filename)
        return {
            "status": "success",
            "data": {
                "filename": filename,
                "psd_number": psd_number
            }
        }
    except ValueError as e:
        logger.error(f"Error extracting PSD from {filename}: {str(e)}")
        raise HTTPException(status_code=400, detail={
            "status": "error",
            "message": str(e)
        })
    except Exception as e:
        logger.error(f"Unexpected error processing {filename}: {str(e)}")
        raise HTTPException(status_code=500, detail={
            "status": "error",
            "message": "An unexpected error occurred"
        })

@router.get("/id", response_model=dict, summary="Get parent ID by PSD number")
async def get_id(
    psd_number: str = Query(
        ...,
        description="PSD number in format 'SFDC-PSD-{number}'",
        examples={"example1": {"value": "SFDC-PSD-076858"}}
    )
):
    """
    Fetch the parent ID associated with a given PSD number.
    
    The PSD number should be in the format: SFDC-PSD-{number}
    """
    try:
        parent_id = get_parent_id_by_psd(psd_number)
        if not parent_id:
            return JSONResponse(
                status_code=404,
                content={
                    "status": "not_found",
                    "message": f"No parent ID found for PSD: {psd_number}"
                }
            )
            
        return {
            "status": "success",
            "data": {
                "psd_number": psd_number,
                "parent_id": parent_id
            }
        }
    except Exception as e:
        logger.error(f"Error fetching parent ID for PSD {psd_number}: {str(e)}")
        raise HTTPException(status_code=500, detail={
            "status": "error",
            "message": f"Failed to fetch parent ID: {str(e)}"
        })
