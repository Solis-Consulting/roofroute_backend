from flask import Flask, request, jsonify, send_file
import os, uuid
import geopandas as gpd
import pandas as pd

app = Flask(__name__)

UPLOAD_FOLDER = "uploads"
OUTPUT_FOLDER = "outputs"
SHAPEFILE_FOLDER = "shapefiles"

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

@app.route("/analyze", methods=["POST"])
def analyze():
    if 'storm' not in request.files or 'county' not in request.form:
        return jsonify({"error": "Missing storm file or county field"}), 400

    county = request.form['county'].lower()
    storm_file = request.files['storm']

    storm_id = str(uuid.uuid4())
    storm_path = os.path.join(UPLOAD_FOLDER, f"{storm_id}.geojson")
    storm_file.save(storm_path)

    try:
        storm_gdf = gpd.read_file(storm_path)
    except Exception as e:
        return jsonify({"error": f"Invalid storm geojson: {e}"}), 400

    county_dir = os.path.join(SHAPEFILE_FOLDER, county)
    if not os.path.exists(county_dir):
        return jsonify({"error": f"No shapefiles found for county '{county}'"}), 404

    shp_files = [f for f in os.listdir(county_dir) if f.endswith(".shp")]
    if not shp_files:
        return jsonify({"error": f"No .shp file found in {county_dir}"})

    shp_path = os.path.join(county_dir, shp_files[0])
    try:
        parcel_gdf = gpd.read_file(shp_path)
    except Exception as e:
        return jsonify({"error": f"Invalid shapefile: {e}"}), 500

    if parcel_gdf.crs != storm_gdf.crs:
        storm_gdf = storm_gdf.to_crs(parcel_gdf.crs)

    try:
        joined = gpd.sjoin(parcel_gdf, storm_gdf, predicate='intersects')
    except Exception as e:
        return jsonify({"error": f"Spatial join failed: {e}"}), 500

    # === Filters
    joined = joined[~joined['OWNNAME'].str.upper().str.contains(
        "LLC|INC|CORP|TRUST|COMPANY|PROPERTIES|ENTERPRISE|INVESTMENTS|HOLDINGS", na=False)]
    joined = joined[~joined['MAILADD'].astype(str).str.startswith("0 ")]
    joined['STRUCTYEAR'] = pd.to_numeric(joined['STRUCTYEAR'], errors='coerce')
    joined = joined[joined['STRUCTYEAR'].notna() & (joined['STRUCTYEAR'] < 1995)]

    # === Projection fix for lat/lon
    joined = joined.to_crs(epsg=4326)

    # === New fields
    joined['lon'] = joined.geometry.centroid.x
    joined['lat'] = joined.geometry.centroid.y
    joined['streetName'] = joined['SITEADD'].astype(str).str.replace(r'^\d+\s+', '', regex=True)

    # === Rename to Swift field names
    joined.rename(columns={
        'OWNNAME': 'owner',
        'MAILADD': 'address',
        'SCITY': 'city',
        'SZIP': 'zip',
        'STRUCTYEAR': 'yearBuilt',
        'IMPROVVAL': 'improvValue',
        'LANDVAL': 'landValue'
    }, inplace=True)

    output_cols = [
        'owner', 'address', 'city', 'zip', 'yearBuilt',
        'improvValue', 'landValue', 'lat', 'lon', 'streetName'
    ]

    csv_id = str(uuid.uuid4())
    output_path = os.path.join(OUTPUT_FOLDER, f"{county}_{csv_id}.csv")

    try:
        joined[output_cols].to_csv(output_path, index=False)
    except Exception as e:
        return jsonify({"error": f"CSV write failed: {e}"}), 500

    try:
        os.remove(storm_path)
    except:
        pass

    print(f"✅ Output saved: {output_path}")
    return send_file(output_path, as_attachment=True)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5050, debug=True)
