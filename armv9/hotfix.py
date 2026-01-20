# test_html.py
import streamlit as st

st.set_page_config(page_title="HTML Test", layout="wide")

st.markdown("### Test 1: Without unsafe_allow_html")
st.markdown("""
<div style="background: red; color: white; padding: 10px;">
    This should show as text
</div>
""")

st.markdown("### Test 2: With unsafe_allow_html=True")
st.markdown("""
<div style="background: green; color: white; padding: 10px;">
    This should show as a GREEN BOX
</div>
""", unsafe_allow_html=True)

if st.button("Test Button"):
    st.success("Button works!")