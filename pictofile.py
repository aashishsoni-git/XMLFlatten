import os
from PIL import Image
import pillow_heif
import pytesseract
import lxml.etree as ET

# Register HEIC support
pillow_heif.register_heif_opener()

image_folder = "XMLPIC"    # folder where HEIC images are
output_folder = "xml_parts"
os.makedirs(output_folder, exist_ok=True)

image_files = sorted([f for f in os.listdir(image_folder) if f.lower().endswith(('.heic','.jpg','.jpeg','.png'))])

for idx, file in enumerate(image_files, start=1):
    img_path = os.path.join(image_folder, file)
    print(f"OCR processing: {file}")

    # Open and convert HEIC to RGB
    img = Image.open(img_path)
    if img.mode != "RGB":
        img = img.convert("RGB")

    # 🔹 Force convert to a format pytesseract supports
    import io
    tmp_png = io.BytesIO()
    img.save(tmp_png, format="PNG")
    tmp_png.seek(0)

    text = pytesseract.image_to_string(Image.open(tmp_png), config="--psm 6")


    # Wrap in Root to make XML-valid
    wrapped_xml = f"<Root>\n{text}\n</Root>"

    # Try to clean with lxml
    try:
        parser = ET.XMLParser(recover=True)
        root = ET.fromstring(wrapped_xml.encode("utf-8"), parser=parser)
        final_xml = ET.tostring(root, pretty_print=True, encoding="utf-8").decode("utf-8")
    except Exception:
        final_xml = wrapped_xml

    # Save
    part_file = os.path.join(output_folder, f"part_{idx}.xml")
    with open(part_file, "w", encoding="utf-8") as f:
        f.write(final_xml)

    print(f"✅ Saved {part_file}")
