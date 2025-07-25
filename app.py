from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import os
import uuid
import geopandas as gpd
import pandas as pd
from io import StringIO

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})

# Configuration
UPLOAD_FOLDER = "uploads"
SHAPEFILE_FOLDER = "shapefiles"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

@app.route('/api/health', methods=['GET'])
def health_check():
    """Endpoint for service health monitoring"""
    return jsonify({
        "status": "healthy",
        "service": "roofroute_backend",
        "version": "1.0"
    }), 200

@app.route('/api/analyze', methods=['POST'])
def analyze():
    """Main analysis endpoint for storm impact assessment"""
    # Validate request
    if 'storm' not in request.files or 'county' not in request.form:
        return jsonify({"error": "Missing storm file or county parameter"}), 400

    county = request.form['county'].lower()
    storm_file = request.files['storm']

    # Save uploaded file
    storm_id = str(uuid.uuid4())
    storm_path = os.path.join(UPLOAD_FOLDER, f"{storm_id}.geojson")
    storm_file.save(storm_path)

    try:
        # Load data
        storm_gdf = gpd.read_file(storm_path)
        county_dir = os.path.join(SHAPEFILE_FOLDER, county)
        shp_path = os.path.join(county_dir, f"nc_{county}_parcels_poly.shp")
        
        if not os.path.exists(shp_path):
            return jsonify({"error": f"County data not available: {county}"}), 404

        parcel_gdf = gpd.read_file(shp_path)

        # Spatial processing
        if parcel_gdf.crs != storm_gdf.crs:
            storm_gdf = storm_gdf.to_crs(parcel_gdf.crs)

        joined = gpd.sjoin(parcel_gdf, storm_gdf, predicate='intersects')

        # Filter results
        joined = joined[
            (~joined['OWNNAME'].str.upper().str.contains(
                "LLC|INC|CORP|TRUST|COMPANY|PROPERTIES",
                na=False)) &
            (~joined['MAILADD'].astype(str).str.startswith("0 ")) &
            (pd.to_numeric(joined['STRUCTYEAR'], errors='coerce') > 0) &
            (pd.to_numeric(joined['STRUCTYEAR'], errors='coerce') < 2000)
        ]

        # Prepare output
        joined = joined.to_crs(epsg=4326)
        joined['lon'] = joined.geometry.centroid.x
        joined['lat'] = joined.geometry.centroid.y
        joined['streetName'] = joined['SITEADD'].astype(str).str.replace(r'^\d+\s+', '', regex=True)

        # Generate CSV
        output = joined.rename(columns={
            'OWNNAME': 'owner',
            'MAILADD': 'address',
            'SCITY': 'city',
            'SZIP': 'zip',
            'STRUCTYEAR': 'yearBuilt',
            'IMPROVVAL': 'improvValue',
            'LANDVAL': 'landValue'
        })[['owner', 'address', 'city', 'zip', 'yearBuilt',
            'improvValue', 'landValue', 'lat', 'lon', 'streetName']]

        csv_buffer = StringIO()
        output.to_csv(csv_buffer, index=False)
        
        return csv_buffer.getvalue(), 200, {
            'Content-Type': 'text/csv',
            'Content-Disposition': 'attachment; filename=parcels.csv'
        }

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        if os.path.exists(storm_path):
            os.remove(storm_path)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
