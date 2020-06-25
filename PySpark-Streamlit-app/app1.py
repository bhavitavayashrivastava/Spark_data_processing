# -*- coding: utf-8 -*-
"""
Created on 21st June 2020

@ Author : Bhavitavaya Praskash Shrivastava
"""
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
import streamlit as st
import PIL as Image

# app = Flask(__name__)

# file1 = request.args.get('file.txt')
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext


def word_count():
    linesRaw = open('file.txt').read().splitlines()
    lines = sc.parallelize(linesRaw)
    words = lines.flatMap(lambda x: x.split(" "))
    wC = words.countByValue()
    return wC


def your_text(text):
    text = list((text).split())
    test = sc.parallelize(text)
    words = test.flatMap(lambda x: x.split(" "))
    wordMap = words.map(lambda x: (x, 1))
    textCount = wordMap.countByKey()
    return textCount


def get_file():
    return '<h1>Will Return word Count</h1>'


count = {}
tc = {}


def main():
    global count
    global tc
    st.title("PySpark Word Count Application")
    html_temp = """
    <div style="background-color:tomato;padding:10px">
    <h2 style="color:white;text-align:center;">Streamlit PySpark Word-Count </h2>
    </div>
    """
    st.markdown(html_temp, unsafe_allow_html=True)
    if st.button("Word Count"):
        count = word_count()
    st.success("The Output is {}".format(count))
    if st.button("Home"):
        st.text("Successfully counted words with PySpark")
    my_text = st.text_input("Enter you Text", "type here")
    if st.button("Your word count"):
        tc = your_text(my_text)
    st.success("Word Count for the text entered {}".format(tc))


if __name__ == '__main__':
    main()
