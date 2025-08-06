import click
import pymysql
from main import create_app
from main.user_managment.services.user_service import UserService
from main.utils.extensions import db

app = create_app()


@app.cli.command("init-db")
def init_db():
    """Initialize the database."""
    # Create database if it doesn't exist
    conn = pymysql.connect(host='localhost', user='root', password='12345')
    cursor = conn.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS projet_pfe")
    conn.commit()
    conn.close()

    # Create tables and add admin user
    with app.app_context():
        db.create_all()
        UserService.add_default_admin_user()

    click.echo("Database initialized successfully.")


if __name__ == "__main__":
    app.run(host="localhost", debug=False)