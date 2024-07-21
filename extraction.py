import requests
import textract
import re
import pandas as pd
import io

# Step 1: Download the PDF
def download_pdf(url, filename):
    """
    Download a PDF file from the given URL and save it locally.
    
    :param url: URL of the PDF file to download
    :param filename: Name of the file to save the PDF as
    """
    try:
        # Send a GET request to the URL
        response = requests.get(url, stream=True)
        
        # Raise an exception for bad status codes
        response.raise_for_status()
        
        # Check if the content type is PDF
        if 'application/pdf' not in response.headers.get('Content-Type', '').lower():
            raise ValueError("The URL does not point to a PDF file.")
        
        # Open the file in binary write mode and save the content
        with open(filename, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file.write(chunk)
        
        print(f"Successfully downloaded {filename}")
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while downloading the file: {e}")
    except ValueError as e:
        print(f"Error: {e}")

# Step 2: Extract text from PDF
# def extract_text(filename):
#     # Use textract with specific method for PDFs
#     text = textract.process(filename, method='pdfminer').decode('utf-8')
#     print("Text extracted from PDF")
#     return text



def extract_text_from_pdf(filename):
    from pdfminer.high_level import extract_text
    text = extract_text(filename)
    return text

# Step 3: Clean and segment text into paragraphs
def clean_and_segment(text):
    # Remove headers, footers, and page numbers
    text = re.sub(r'(?m)^\d+$', '', text)  # Remove standalone page numbers
    text = re.sub(r'(?m)^.*?SUSTAINABLE FINANCE.*?$', '', text, flags=re.IGNORECASE)  # Remove headers
    
    # Remove newlines and extra spaces
    text = re.sub(r'\s+', ' ', text)
    
    # Split into paragraphs
    paragraphs = re.split(r'(?<=[.!?])\s+(?=[A-Z])', text)
    
    # Clean paragraphs
    cleaned_paragraphs = []
    for para in paragraphs:
        # Remove leading/trailing whitespace
        para = para.strip()
        
        # Remove any remaining special characters or unwanted patterns
        para = re.sub(r'[^\w\s.,:;()\-–—]', '', para)
        
        # Remove short phrases that might be leftover from headers/footers
        if len(para.split()) > 5 and len(para) >= 200:
            cleaned_paragraphs.append(para)
    
    print(f"Extracted {len(cleaned_paragraphs)} paragraphs")
    return cleaned_paragraphs

# Step 4: Store paragraphs in DataFrame
def create_dataframe(paragraphs):
    df = pd.DataFrame(paragraphs, columns=['paragraph'])
    print("Created DataFrame with paragraphs")
    return df

# Main execution
if __name__ == "__main__":
    url = "https://lp-prod-resources.s3.us-west-2.amazonaws.com/214/200309-sustainable-finance-teg-final-report-taxonomy-annexes_en.pdf"
    filename = "eu_taxonomy.pdf"
    
    download_pdf(url, filename)
    raw_text = extract_text_from_pdf(filename)
    cleaned_paragraphs = clean_and_segment(raw_text)
    df = create_dataframe(cleaned_paragraphs)
    
    # Save DataFrame to CSV
    df.to_csv("eu_taxonomy_paragraphs.csv", index=False)
    print("Saved paragraphs to eu_taxonomy_paragraphs.csv")
    
    # Print first few paragraphs for verification
    print("\nFirst few paragraphs:")
    print(df.head())