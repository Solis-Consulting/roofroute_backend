RoofRoute Backend Instructions

1. Run: pip install -r requirements.txt
2. Start: python app.py
3. POST to http://localhost:5000/analyze with:
   - storm: .geojson file (from drawn area)
   - county: e.g., "orange" or "durham"
4. Receives filtered CSV back