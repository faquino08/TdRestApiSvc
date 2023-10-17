#!/bin/sh
gunicorn --bind=0.0.0.0:18080 wsgi:app --timeout 600