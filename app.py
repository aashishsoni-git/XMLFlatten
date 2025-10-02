import streamlit as st
import xml.etree.ElementTree as ET
import pandas as pd
import itertools
import io


def expand_node(element, context, path=""):
    """
    Expand XML recursively into rows with structured column names.
    - Column names become Parent_Child_Attribute
    """
    base = context.copy()
    prefix = f"{path}_{element.tag}" if path else element.tag

    # Add attributes
    for k, v in element.attrib.items():
        col_name = f"{prefix}_{k}"
        base[col_name] = v

    # Add text if simple leaf
    text = (element.text or "").strip()
    if text and len(element) == 0:
        base[prefix] = text
        return [base]

    children = list(element)
    if not children:
        return [base]

    # group children by tag
    groups = {}
    for child in children:
        groups.setdefault(child.tag, []).append(child)

    # recurse each group
    expanded_groups = []
    for tag, nodes in groups.items():
        rows = []
        for n in nodes:
            rows.extend(expand_node(n, base, prefix))
        expanded_groups.append(rows)

    # cartesian product across all groups
    rows = []
    for combo in itertools.product(*expanded_groups):
        merged = base.copy()
        for row in combo:
            merged.update(row)
        rows.append(merged)

    return rows


def parse_xml(xml_string):
    root = ET.fromstring(xml_string)
    rows = expand_node(root, {}, "")
    return pd.DataFrame(rows)


# ---------------- Streamlit UI ----------------
st.title("📂 XML Relational Flattener (Custom Column Names)")
st.write("""
Upload XML → Expands repeating groups into rows.  
Columns use structured naming like `Parent_Child_Attribute`.
""")

uploaded_file = st.file_uploader("Upload XML File", type=["xml"])

if uploaded_file is not None:
    try:
        xml_content = uploaded_file.read().decode("utf-8")
        df = parse_xml(xml_content)

        st.success(f"✅ Parsed successfully! Generated {len(df)} rows.")

        st.subheader("📊 Flattened XML Table (first 50 rows)")
        st.dataframe(df.head(50), use_container_width=True)

        # Download CSV
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        st.download_button(
            label="⬇️ Download full CSV",
            data=csv_buffer.getvalue(),
            file_name="flattened_xml.csv",
            mime="text/csv",
        )

    except Exception as e:
        st.error(f"❌ Error parsing XML: {e}")
