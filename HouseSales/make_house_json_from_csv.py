import pandas as pd, json

def make_house_json_from_csv(
    csv_path="us_house_Sales_data 2.csv",
    json_path="house_sales_data.json",
    subset_json_path="house_sales_subset.json",
    subset_rows=12
):
    df = pd.read_csv(csv_path).fillna("")

    out = []
    for _, r in df.iterrows():
        out.append([
            str(r.get("Price","")).strip(),
            str(r.get("Address","")).strip(),
            str(r.get("City","")).strip(),
            str(r.get("Zipcode","")).strip(),
            str(r.get("State","")).strip(),
            str(r.get("Bedrooms","")).strip(),
            str(r.get("Bathrooms","")).strip(),
            str(r.get("Area (sqft)","")).strip(),
            str(r.get("Lot Size","")).strip(),
            str(r.get("Year Built","")).strip(),
            str(r.get("Days on Market","")).strip(),
            str(r.get("Property Type","")).strip(),
            str(r.get("MLS ID","")).strip(),
            str(r.get("Listing Agent","")).strip(),
            str(r.get("Status","")).strip(),
            str(r.get("Listing URL","")).strip()
        ])

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    with open(subset_json_path, "w", encoding="utf-8") as f:
        json.dump(out[:subset_rows], f, ensure_ascii=False, indent=2)

    print(f"Made {json_path} (full: {len(out)} rows)")
    print(f"Made {subset_json_path} (subset: {min(len(out), subset_rows)} rows)")

make_house_json_from_csv()
