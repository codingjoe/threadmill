import os
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parents[2]))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tests.testapp.settings")


def main():
    """Run administrative commands against the test app settings."""
    from django.core.management import execute_from_command_line

    execute_from_command_line(sys.argv)


if __name__ == "__main__":
    main()
