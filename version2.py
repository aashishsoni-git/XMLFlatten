import xml.etree.ElementTree as ET
import pandas as pd
import itertools

def expand_node(element, context):
    """
    Expand XML recursively into rows.
    - Carries parent attributes/text down.
    - For repeating groups, produces a cartesian product.
    """
    base = context.copy()

    # Add attributes
    for k, v in element.attrib.items():
        base[f"{element.tag}_@{k}"] = v

    # Add text if simple leaf
    text = (element.text or "").strip()
    if text and len(element) == 0:
        base[element.tag] = text
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
            rows.extend(expand_node(n, base))
        expanded_groups.append(rows)

    # now cartesian product across all groups
    rows = []
    for combo in itertools.product(*expanded_groups):
        merged = base.copy()
        for row in combo:
            merged.update(row)
        rows.append(merged)

    return rows


def parse_xml(xml_string):
    root = ET.fromstring(xml_string)
    rows = expand_node(root, {})
    return pd.DataFrame(rows)


if __name__ == "__main__":
    # --- Load your XML file ---
    with open("insurance.xml", "r", encoding="utf-8") as f:
        xml_string = f.read()

    df = parse_xml(xml_string)

    # Print summary
    print("Number of rows:", len(df))
    print(df.head(20))

    # Save to CSV
    df.to_csv("flattened_output.csv", index=False)
    print("✅ Flattened CSV written to flattened_output.csv")
