import click
import pymysql
from flask import Flask
from main import create_app
from main.user_managment.services.user_service import UserService
from main.utils.extensions import db
import os

app = create_app()


@app.cli.command("init-db")
def init_db():
    """Initialize the database."""
    try:
        # Get database connection parameters from environment variables
        db_host = os.getenv('DB_HOST', 'localhost')
        db_user = os.getenv('DB_USER', 'root')
        db_password = os.getenv('DB_PASSWORD', '12345')
        db_name = os.getenv('DB_NAME', 'projet_pfe')

        # Create database if it doesn't exist
        conn = pymysql.connect(
            host=db_host,
            user=db_user,
            password=db_password,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )

        try:
            with conn.cursor() as cursor:
                cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
                cursor.execute(f"ALTER DATABASE {db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            conn.commit()
            click.echo(f"Database '{db_name}' created or verified successfully.")
        finally:
            conn.close()

        # Create tables and add admin user
        with app.app_context():
            db.create_all()
            try:
                UserService.add_default_admin_user()
                click.echo("Admin user created successfully.")
            except Exception as admin_error:
                click.echo(f"Error creating admin user: {str(admin_error)}")

        click.echo("Database initialization completed successfully.")

    except pymysql.Error as db_error:
        click.echo(f"Database connection error: {str(db_error)}")
    except Exception as e:
        click.echo(f"Unexpected error during initialization: {str(e)}")


@app.cli.command("reset-db")
def reset_db():
    """Reset the database (drop and recreate)."""
    if click.confirm('Are you sure you want to reset the database? All data will be lost.'):
        try:
            # Get database connection parameters
            db_host = os.getenv('DB_HOST', 'localhost')
            db_user = os.getenv('DB_USER', 'root')
            db_password = os.getenv('DB_PASSWORD', '12345')
            db_name = os.getenv('DB_NAME', 'projet_pfe')

            # Connect to MySQL server
            conn = pymysql.connect(
                host=db_host,
                user=db_user,
                password=db_password,
                charset='utf8mb4'
            )

            try:
                with conn.cursor() as cursor:
                    cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
                    cursor.execute(f"CREATE DATABASE {db_name}")
                    cursor.execute(f"ALTER DATABASE {db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
                conn.commit()
                click.echo(f"Database '{db_name}' reset successfully.")
            finally:
                conn.close()

            # Recreate tables and admin user
            with app.app_context():
                db.create_all()
                UserService.add_default_admin_user()
                click.echo("Tables and admin user recreated successfully.")

        except Exception as e:
            click.echo(f"Error resetting database: {str(e)}")


if __name__ == "__main__":
    app.run(host="0.0.0.0" if os.getenv('DOCKERIZED') else "localhost",
            port=int(os.getenv('PORT', 5000)),
            debug=os.getenv('FLASK_DEBUG', 'false').lower() == 'true')