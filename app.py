from flask import Flask, request, jsonify
import os, uuid
import geopandas as gpd
import pandas as pd

app = Flask(__name__)

UPLOAD_FOLDER = "uploads"
SHAPEFILE_FOLDER = "shapefiles"

os.makedirs(UPLOAD_FOLDER, exist_ok=True)

@app.route("/analyze", methods=["POST"])
def analyze():
    if 'storm' not in request.files or 'county' not in request.form:
        print("❌ Missing storm file or county field")
        return jsonify({"error": "Missing storm file or county field"}), 400

    county = request.form['county'].lower()
    print(f"📥 Received analysis request for county: '{county}'")
    storm_file = request.files['storm']

    # Save uploaded storm file
    storm_id = str(uuid.uuid4())
    storm_path = os.path.join(UPLOAD_FOLDER, f"{storm_id}.geojson")
    storm_file.save(storm_path)
    print(f"📝 Saved storm file to: {storm_path}")

    # Load storm GeoJSON
    try:
        storm_gdf = gpd.read_file(storm_path)
    except Exception as e:
        print(f"❌ Invalid storm geojson: {e}")
        return jsonify({"error": f"Invalid storm geojson: {e}"}), 400

    # Load county shapefile
    county_dir = os.path.join(SHAPEFILE_FOLDER, county)
    if not os.path.exists(county_dir):
        print(f"❌ No shapefile directory found for county: '{county}' → Path: {county_dir}")
        return jsonify({"error": f"No shapefiles found for county '{county}'"}), 404

    shp_files = [f for f in os.listdir(county_dir) if f.endswith(".shp")]
    if not shp_files:
        print(f"❌ No .shp file found in {county_dir}")
        return jsonify({"error": f"No .shp file found in {county_dir}"}), 500

    shp_path = os.path.join(county_dir, shp_files[0])
    print(f"📦 Using shapefile: {shp_path}")
    try:
        parcel_gdf = gpd.read_file(shp_path)
    except Exception as e:
        print(f"❌ Invalid shapefile: {e}")
        return jsonify({"error": f"Invalid shapefile: {e}"}), 500

    # Reproject if needed
    if parcel_gdf.crs != storm_gdf.crs:
        print("🔁 Reprojecting storm GeoJSON to match parcel CRS")
        storm_gdf = storm_gdf.to_crs(parcel_gdf.crs)

    # Spatial join
    try:
        joined = gpd.sjoin(parcel_gdf, storm_gdf, predicate='intersects')
    except Exception as e:
        print(f"❌ Spatial join failed: {e}")
        return jsonify({"error": f"Spatial join failed: {e}"}), 500

    print(f"✅ Parcels intersected: {len(joined)}")

    # Filtering
    joined = joined[~joined['OWNNAME'].str.upper().str.contains(
        "LLC|INC|CORP|TRUST|COMPANY|PROPERTIES|ENTERPRISE|INVESTMENTS|HOLDINGS", na=False)]
    joined = joined[~joined['MAILADD'].astype(str).str.startswith("0 ")]
    joined['STRUCTYEAR'] = pd.to_numeric(joined['STRUCTYEAR'], errors='coerce')
    joined = joined[joined['STRUCTYEAR'].notna() & (joined['STRUCTYEAR'] < 1995)]

    # Convert to lat/lon
    joined = joined.to_crs(epsg=4326)
    joined['lon'] = joined.geometry.centroid.x
    joined['lat'] = joined.geometry.centroid.y
    joined['streetName'] = joined['SITEADD'].astype(str).str.replace(r'^\d+\s+', '', regex=True)

    # Rename columns
    joined.rename(columns={
        'OWNNAME': 'owner',
        'MAILADD': 'address',
        'SCITY': 'city',
        'SZIP': 'zip',
        'STRUCTYEAR': 'yearBuilt',
        'IMPROVVAL': 'improvValue',
        'LANDVAL': 'landValue'
    }, inplace=True)

    # Prepare final output
    output_cols = [
        'owner', 'address', 'city', 'zip', 'yearBuilt',
        'improvValue', 'landValue', 'lat', 'lon', 'streetName'
    ]

    try:
        csv_string = joined[output_cols].to_csv(index=False)
    except Exception as e:
        print(f"❌ CSV conversion failed: {e}")
        return jsonify({"error": f"CSV conversion failed: {e}"}), 500

    try:
        os.remove(storm_path)
    except Exception as e:
        print(f"⚠️ Could not delete storm file: {e}")

    print("✅ Returning final CSV")
    return csv_string, 200, {
        "Content-Type": "text/csv",
        "Content-Disposition": "inline"
    }

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
