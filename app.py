from __future__ import annotations

import os

from controle_separacao.core import app, ensure_default_data


if __name__ == "__main__":
    ensure_default_data()
    port = int(os.environ.get("PORT", "5000"))
    debug = os.environ.get("FLASK_DEBUG", "0") == "1"
    app.run(host="0.0.0.0", port=port, debug=debug)
