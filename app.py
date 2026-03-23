import streamlit as st
import requests
import os
from dotenv import load_dotenv

load_dotenv()

API_URL = os.getenv("API_URL", "http://localhost:8001")

st.set_page_config(page_title="Real-Time Embedding Explorer", page_icon="⚡")
st.title("⚡ Real-Time Embedding Explorer")
st.markdown("Explore user embeddings computed in real-time.")

option = st.selectbox("Choose action", ["Get user embedding", "Find similar users"])

if option == "Get user embedding":
    user_id = st.text_input("User ID")
    if st.button("Get embedding"):
        if user_id:
            resp = requests.get(f"{API_URL}/embedding/{user_id}")
            if resp.status_code == 200:
                data = resp.json()
                st.write("Embedding (first 10 dimensions):", data["embedding"][:10])
            else:
                st.error(f"Error: {resp.status_code}")

elif option == "Find similar users":
    user_id = st.text_input("User ID")
    top_k = st.number_input("Top K", min_value=1, max_value=20, value=5)
    if st.button("Find similar"):
        if user_id:
            resp = requests.post(f"{API_URL}/similar", json={"user_id": user_id, "top_k": top_k})
            if resp.status_code == 200:
                data = resp.json()
                st.write("Similar users:", data["similar_users"])
            else:
                st.error(f"Error: {resp.status_code}")