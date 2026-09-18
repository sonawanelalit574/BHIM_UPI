"""Run the BHIM UPI administrative console (simulation).

    python run_upi_admin.py

Open http://127.0.0.1:5070  — default token: dev-admin
"""

import os

from upi_admin import create_admin_app

app = create_admin_app()

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=int(os.environ.get("UPI_ADMIN_PORT", 5070)), debug=True)
