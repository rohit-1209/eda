from flask import Flask
from flask_cors import CORS
import warnings
import jwt

# Register blueprints
from route.routes import main  # This should work now
from helpers.change_datatype import CustomJSONEncoder


def create_app():
    app = Flask(__name__)
    app.json_encoder = CustomJSONEncoder
    app.url_map.strict_slashes = False
    app.config['SECRET_KEY'] = 'ZfvEbWyeHoGbcNYYs-o'
    # CORS(app,
    #      supports_credentials=False,
    #      resources={r"/*": {"origins": "http://localhost:3000"}}
    #      )
    CORS(app,
         supports_credentials=True,
         resources={r"/*": {"origins": "*"}}
         )

    warnings.simplefilter('ignore')

    # app.register_blueprint(main, url_prefix='')

    return app


app = create_app()
app.register_blueprint(main, url_prefix='')

if __name__ == "__main__":
    app.run(host="0.0.0.0", threaded=True, port=8000, debug=False)
