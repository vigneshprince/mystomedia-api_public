FROM python:3.9-alpine as base
COPY . .
RUN pip --no-cache-dir install -r requirements.txt
ENV SPOTIPY_CLIENT_ID='f776f42ddd3e472680016e9a939476c3'
ENV SPOTIPY_CLIENT_SECRET='69fa57836cfc4110a30e154e977513f2'
CMD uvicorn main:app --host=0.0.0.0 --port=${PORT:-5000}