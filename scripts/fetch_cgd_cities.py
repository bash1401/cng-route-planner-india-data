"""
Fetch ALL fuel stations in CGD (City Gas Distribution) authorized geographic areas.

In PNGRB-authorized CGD areas, CNG dispensing is mandatory at retail fuel outlets,
resulting in high CNG penetration (typically 70-95% of fuel stations). This script
uses that geographic inference to collect candidate CNG stations.

Queries ALL amenity=fuel nodes from Overpass in known CGD cities/districts.
Tags results as source="cgd_inferred" (lower priority than verified sources).

Coverage: ~50 major CGD geographic areas across India
Saves to raw_sources/cgd_cities.json
"""
from __future__ import annotations

import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request

sys.path.insert(0, os.path.dirname(__file__))
from utils import save_raw, haversine_m, state_from_coords, INDIA_STATES, normalise_name

SOURCE = "cgd_inferred"

# CGD-authorized geographic areas with bounding boxes
# Format: (name, state, bbox "lat_min,lon_min,lat_max,lon_max")
# Sources: PNGRB authorizations, operator websites, news reports
_CGD_AREAS: list[tuple[str, str, str]] = [
    # IGL - Delhi NCR
    ("Delhi",           "Delhi",        "28.40,76.85,28.88,77.36"),
    ("Noida/Gr.Noida",  "Uttar Pradesh","28.40,77.30,28.65,77.55"),
    ("Gurgaon",         "Haryana",      "28.35,76.90,28.55,77.10"),
    ("Faridabad",       "Haryana",      "28.30,77.20,28.60,77.50"),
    ("Ghaziabad",       "Uttar Pradesh","28.55,77.30,28.75,77.55"),
    ("Meerut",          "Uttar Pradesh","28.85,77.55,29.10,77.85"),
    ("Muzaffarnagar",   "Uttar Pradesh","29.35,77.55,29.55,77.80"),
    ("Rewari",          "Haryana",      "28.10,76.55,28.30,76.75"),
    ("Karnal",          "Haryana",      "29.55,76.85,29.75,77.05"),
    ("Kangra",          "Himachal Pradesh","31.80,76.20,32.15,76.65"),

    # MGL - Mumbai
    ("Mumbai City",     "Maharashtra",  "18.90,72.78,19.30,73.05"),
    ("Thane",           "Maharashtra",  "19.15,72.95,19.35,73.15"),
    ("Raigad/Navi Mumbai","Maharashtra","18.90,73.00,19.10,73.25"),

    # Gujarat Gas - Gujarat (major areas)
    ("Ahmedabad",       "Gujarat",      "22.85,72.40,23.15,72.75"),
    ("Surat",           "Gujarat",      "21.10,72.70,21.40,73.10"),
    ("Anand",           "Gujarat",      "22.45,72.80,22.65,73.05"),
    ("Vadodara",        "Gujarat",      "22.20,73.10,22.45,73.35"),
    ("Bharuch",         "Gujarat",      "21.55,72.85,21.80,73.10"),
    ("Rajkot",          "Gujarat",      "22.20,70.70,22.40,70.95"),
    ("Gandhinagar",     "Gujarat",      "23.15,72.55,23.35,72.80"),
    ("Mehsana",         "Gujarat",      "23.55,72.30,23.75,72.50"),
    ("Navsari",         "Gujarat",      "20.80,72.85,21.05,73.05"),
    ("Valsad",          "Gujarat",      "20.50,72.85,20.75,73.05"),
    ("Morbi",           "Gujarat",      "22.75,70.70,22.90,70.95"),
    ("Jamnagar",        "Gujarat",      "22.35,70.00,22.55,70.25"),
    ("Amreli",          "Gujarat",      "21.55,71.15,21.75,71.40"),
    ("Bhavnagar",       "Gujarat",      "21.70,72.05,21.90,72.30"),
    ("Junagadh",        "Gujarat",      "21.45,70.35,21.65,70.65"),
    ("Kutch/Gandhidham","Gujarat",      "23.00,70.00,23.30,70.25"),
    ("Vapi/Umbergaon",  "Gujarat",      "20.30,72.85,20.55,73.05"),
    ("Banaskantha",     "Gujarat",      "23.70,72.20,23.90,72.45"),

    # ATGL - Adani Total Gas
    ("Vadodara ATGL",   "Gujarat",      "22.20,73.10,22.45,73.35"),  # overlaps Gujarat Gas
    ("Ahmedabad ATGL",  "Gujarat",      "22.85,72.40,23.15,72.75"),
    ("Khurja",          "Uttar Pradesh","28.20,77.80,28.40,78.00"),
    ("Daman",           "Daman and Diu","20.35,72.85,20.45,72.95"),
    ("Chandrapur",      "Maharashtra",  "20.00,79.15,20.15,79.35"),

    # GAIL Gas GAs
    ("Bengaluru N",     "Karnataka",    "12.85,77.40,13.20,77.75"),
    ("Hyderabad",       "Telangana",    "17.25,78.25,17.60,78.65"),
    ("Ranchi",          "Jharkhand",    "23.20,85.20,23.50,85.50"),
    ("Jamshedpur",      "Jharkhand",    "22.70,86.10,22.95,86.35"),
    ("Agra",            "Uttar Pradesh","26.95,77.90,27.25,78.20"),
    ("Mathura-Vrindavan","Uttar Pradesh","27.40,77.55,27.65,77.80"),
    ("Haridwar/Roorkee", "Uttarakhand", "29.85,77.85,29.95,78.10"),
    ("Dehradun",        "Uttarakhand",  "30.25,77.90,30.40,78.10"),
    ("Sundargarh/Rourkela","Odisha",   "22.15,84.75,22.35,84.95"),
    ("Sambalpur",       "Odisha",       "21.40,83.90,21.60,84.10"),
    ("Meerut/Ghaziabad2","Uttar Pradesh","28.80,77.40,29.05,77.75"),
    ("Varanasi",        "Uttar Pradesh","25.25,82.80,25.45,83.05"),
    ("Kanpur",          "Uttar Pradesh","26.35,80.20,26.60,80.45"),

    # MNGL - Maharashtra Natural Gas
    ("Pune",            "Maharashtra",  "18.40,73.70,18.65,74.00"),
    ("Nashik",          "Maharashtra",  "19.90,73.65,20.10,73.90"),
    ("Nanded",          "Maharashtra",  "19.05,77.25,19.25,77.45"),
    ("Ramanagara",      "Karnataka",    "12.65,77.20,12.85,77.40"),
    ("Sindhudurg",      "Maharashtra",  "16.00,73.45,16.20,73.65"),

    # Green Gas - Lucknow
    ("Lucknow",         "Uttar Pradesh","26.75,80.80,26.95,81.05"),

    # Haryana City Gas
    ("Ambala",          "Haryana",      "30.30,76.70,30.55,76.90"),
    ("Yamunanagar",     "Haryana",      "30.05,77.20,30.25,77.40"),
    ("Panipat",         "Haryana",      "29.30,76.90,29.50,77.10"),
    ("Sonipat",         "Haryana",      "28.90,76.95,29.10,77.15"),
    ("Rohtak",          "Haryana",      "28.80,76.55,29.00,76.75"),
    ("Hisar",           "Haryana",      "29.10,75.65,29.25,75.85"),

    # Torrent Gas - Gujarat + others
    ("Gandhinagar/Kalol","Gujarat",     "23.20,72.50,23.45,72.75"),
    ("Ahmedabad East",  "Gujarat",      "23.00,72.60,23.20,72.85"),
    ("Dharuhera/Bhiwadi","Rajasthan",   "27.95,76.65,28.15,76.90"),

    # AG&P Pratham
    ("Chennai North",   "Tamil Nadu",   "13.05,80.10,13.25,80.35"),
    ("Thane/Ulhasnagar","Maharashtra",  "19.15,73.05,19.35,73.25"),
    ("Palghar",         "Maharashtra",  "19.65,72.75,19.90,73.00"),

    # Central UP Gas (CUGL)
    ("Kanpur CUGL",     "Uttar Pradesh","26.35,80.20,26.60,80.45"),
    ("Bareilly",        "Uttar Pradesh","28.30,79.30,28.50,79.55"),

    # Punjab CGD
    ("Amritsar",        "Punjab",       "31.55,74.80,31.75,75.05"),
    ("Ludhiana",        "Punjab",       "30.80,75.75,31.00,76.00"),
    ("Jalandhar",       "Punjab",       "31.25,75.50,31.45,75.75"),
    ("Patiala",         "Punjab",       "30.25,76.30,30.45,76.55"),
    ("Bathinda",        "Punjab",       "29.95,74.90,30.15,75.15"),
    ("Chandigarh",      "Chandigarh",   "30.62,76.68,30.79,76.89"),
    ("Mohali/Rupnagar", "Punjab",       "30.70,76.60,30.90,76.90"),

    # Rajasthan
    ("Jaipur",          "Rajasthan",    "26.75,75.60,27.00,75.90"),
    ("Kota",            "Rajasthan",    "25.10,75.75,25.30,76.00"),
    ("Bikaner",         "Rajasthan",    "28.00,73.25,28.20,73.50"),
    ("Alwar",           "Rajasthan",    "27.50,76.55,27.70,76.75"),

    # MP
    ("Indore",          "Madhya Pradesh","22.55,75.75,22.80,76.05"),
    ("Bhopal",          "Madhya Pradesh","23.15,77.25,23.35,77.50"),
    ("Ujjain",          "Madhya Pradesh","23.10,75.65,23.30,75.85"),
    ("Dewas",           "Madhya Pradesh","22.85,76.00,23.05,76.20"),

    # West Bengal / Bihar
    ("Kolkata",         "West Bengal",  "22.45,88.20,22.75,88.50"),
    ("Patna",           "Bihar",        "25.50,85.00,25.70,85.25"),
    ("Dhanbad",         "Jharkhand",    "23.70,86.35,23.90,86.55"),

    # South India additional
    ("Mangalore",       "Karnataka",    "12.80,74.80,13.00,75.05"),
    ("Mysuru",          "Karnataka",    "12.25,76.55,12.45,76.75"),
    ("Kochi",           "Kerala",       "9.90,76.25,10.10,76.45"),
    ("Coimbatore",      "Tamil Nadu",   "10.95,76.90,11.15,77.10"),
    ("Visakhapatnam",   "Andhra Pradesh","17.55,83.10,17.80,83.40"),
    ("Vijayawada",      "Andhra Pradesh","16.45,80.55,16.65,80.75"),
    ("Tirupati",        "Andhra Pradesh","13.55,79.30,13.75,79.50"),
    ("Vizianagaram",    "Andhra Pradesh","18.05,83.35,18.25,83.55"),
    ("Warangal",        "Telangana",    "17.85,79.50,18.05,79.75"),
    ("Nizamabad",       "Telangana",    "18.60,78.00,18.80,78.20"),

    # Assam/NE
    ("Guwahati",        "Assam",        "26.05,91.55,26.25,91.85"),
    ("Agartala",        "Tripura",      "23.75,91.20,23.95,91.40"),
    ("Dibrugarh",       "Assam",        "27.40,94.80,27.60,95.05"),

    # Tamil Nadu - major CGD cities
    ("Chennai Central", "Tamil Nadu",   "12.95,80.15,13.10,80.35"),
    ("Chennai South",   "Tamil Nadu",   "12.80,80.10,13.00,80.30"),
    ("Chennai West",    "Tamil Nadu",   "13.00,80.05,13.25,80.20"),
    ("Madurai",         "Tamil Nadu",   "9.85,78.00,10.05,78.20"),
    ("Trichy/Tiruchirappalli","Tamil Nadu","10.70,78.55,10.90,78.80"),
    ("Salem",           "Tamil Nadu",   "11.55,78.00,11.75,78.25"),
    ("Tirunelveli",     "Tamil Nadu",   "8.65,77.60,8.85,77.80"),
    ("Vellore",         "Tamil Nadu",   "12.85,79.05,13.05,79.30"),
    ("Tiruppur",        "Tamil Nadu",   "11.05,77.25,11.25,77.45"),
    ("Erode",           "Tamil Nadu",   "11.30,77.65,11.50,77.85"),
    ("Thanjavur",       "Tamil Nadu",   "10.70,79.05,10.90,79.25"),
    ("Puducherry",      "Tamil Nadu",   "11.85,79.75,12.05,79.95"),
    ("Kanchipuram",     "Tamil Nadu",   "12.75,79.65,12.95,79.85"),
    ("Tiruvallur",      "Tamil Nadu",   "13.10,79.85,13.30,80.10"),
    ("Chengalpattu",    "Tamil Nadu",   "12.55,79.90,12.75,80.15"),
    ("Cuddalore",       "Tamil Nadu",   "11.65,79.65,11.85,79.85"),
    ("Nagapattinam",    "Tamil Nadu",   "10.70,79.75,10.90,79.95"),
    ("Kumbakonam",      "Tamil Nadu",   "10.90,79.30,11.10,79.50"),
    ("Dindigul",        "Tamil Nadu",   "10.30,77.85,10.50,78.05"),
    ("Thoothukudi/Tuticorin","Tamil Nadu","8.70,78.05,8.90,78.25"),
    ("Nagercoil",       "Tamil Nadu",   "8.15,77.35,8.35,77.55"),
    ("Karur",           "Tamil Nadu",   "10.85,78.00,11.05,78.20"),

    # Maharashtra - additional CGD cities
    ("Nagpur",          "Maharashtra",  "20.95,78.95,21.20,79.25"),
    ("Aurangabad/Sambhajinagar","Maharashtra","19.80,75.20,20.00,75.45"),
    ("Solapur",         "Maharashtra",  "17.60,75.85,17.80,76.10"),
    ("Kolhapur",        "Maharashtra",  "16.65,74.15,16.85,74.35"),
    ("Amravati",        "Maharashtra",  "20.85,77.65,21.05,77.90"),
    ("Akola",           "Maharashtra",  "20.65,76.95,20.85,77.15"),
    ("Latur",           "Maharashtra",  "18.35,76.45,18.55,76.65"),
    ("Jalgaon",         "Maharashtra",  "21.00,75.50,21.20,75.75"),
    ("Satara",          "Maharashtra",  "17.55,74.00,17.75,74.20"),
    ("Sangli",          "Maharashtra",  "16.80,74.50,17.00,74.70"),
    ("Navi Mumbai",     "Maharashtra",  "19.00,73.00,19.15,73.15"),
    ("Kalyan-Dombivli", "Maharashtra",  "19.20,73.10,19.40,73.30"),
    ("Vasai-Virar",     "Maharashtra",  "19.35,72.80,19.55,72.95"),
    ("Dhule",           "Maharashtra",  "20.85,74.65,21.05,74.85"),
    ("Osmanabad",       "Maharashtra",  "18.10,76.00,18.30,76.20"),
    ("Beed",            "Maharashtra",  "18.90,75.65,19.10,75.85"),
    ("Yavatmal",        "Maharashtra",  "20.35,78.05,20.55,78.25"),
    ("Wardha",          "Maharashtra",  "20.65,78.55,20.85,78.75"),
    ("Bhusawal",        "Maharashtra",  "21.00,75.70,21.20,75.90"),

    # Karnataka - additional CGD cities
    ("Bengaluru South", "Karnataka",    "12.85,77.50,13.00,77.65"),
    ("Bengaluru East",  "Karnataka",    "12.95,77.65,13.10,77.80"),
    ("Hubli-Dharwad",   "Karnataka",    "15.30,75.00,15.50,75.25"),
    ("Belagavi/Belgaum","Karnataka",    "15.80,74.45,16.00,74.65"),
    ("Mysuru South",    "Karnataka",    "12.15,76.60,12.35,76.80"),
    ("Tumkur",          "Karnataka",    "13.30,77.05,13.50,77.25"),
    ("Shivamogga",      "Karnataka",    "13.85,75.45,14.05,75.65"),
    ("Hassan",          "Karnataka",    "13.00,76.00,13.20,76.20"),
    ("Davanagere",      "Karnataka",    "14.40,75.85,14.60,76.05"),
    ("Ballari/Bellary", "Karnataka",    "15.10,76.80,15.30,77.05"),
    ("Bidar",           "Karnataka",    "17.80,77.45,18.00,77.65"),
    ("Raichur",         "Karnataka",    "16.15,77.30,16.35,77.50"),
    ("Kalaburagi/Gulbarga","Karnataka", "17.25,76.75,17.45,76.95"),
    ("Mandya",          "Karnataka",    "12.50,76.85,12.70,77.05"),
    ("Udupi",           "Karnataka",    "13.25,74.65,13.45,74.85"),
    ("Bagalkot",        "Karnataka",    "16.15,75.65,16.35,75.85"),
    ("Gadag",           "Karnataka",    "15.40,75.55,15.60,75.75"),
    ("Vijayapura",      "Karnataka",    "16.80,75.65,17.00,75.85"),

    # Kerala - additional CGD cities
    ("Thiruvananthapuram","Kerala",     "8.40,76.85,8.60,77.05"),
    ("Kozhikode",       "Kerala",       "11.20,75.75,11.40,76.00"),
    ("Thrissur",        "Kerala",       "10.45,76.15,10.65,76.35"),
    ("Kollam",          "Kerala",       "8.80,76.55,9.00,76.75"),
    ("Kannur",          "Kerala",       "11.80,75.30,12.00,75.50"),
    ("Palakkad",        "Kerala",       "10.70,76.55,10.90,76.75"),
    ("Alappuzha",       "Kerala",       "9.45,76.25,9.65,76.45"),
    ("Kottayam",        "Kerala",       "9.55,76.45,9.75,76.65"),
    ("Malappuram",      "Kerala",       "11.00,76.00,11.20,76.20"),
    ("Kasaragod",       "Kerala",       "12.45,74.90,12.65,75.10"),
    ("Pathanamthitta",  "Kerala",       "9.20,76.75,9.40,76.95"),
    ("Idukki/Thodupuzha","Kerala",      "9.75,76.85,9.95,77.05"),

    # Andhra Pradesh - additional CGD cities
    ("Nellore",         "Andhra Pradesh","14.40,79.90,14.60,80.10"),
    ("Guntur",          "Andhra Pradesh","16.20,80.35,16.40,80.55"),
    ("Kurnool",         "Andhra Pradesh","15.75,78.00,15.95,78.20"),
    ("Ongole",          "Andhra Pradesh","15.45,80.00,15.65,80.20"),
    ("Rajahmundry",     "Andhra Pradesh","16.95,81.65,17.15,81.85"),
    ("Eluru",           "Andhra Pradesh","16.65,81.00,16.85,81.20"),
    ("Kakinada",        "Andhra Pradesh","16.85,82.15,17.05,82.35"),
    ("Kadapa",          "Andhra Pradesh","14.40,78.75,14.60,78.95"),
    ("Anantapur",       "Andhra Pradesh","14.60,77.55,14.80,77.75"),
    ("Proddatur",       "Andhra Pradesh","14.65,78.45,14.85,78.65"),
    ("Nandyal",         "Andhra Pradesh","15.40,78.35,15.60,78.55"),

    # Telangana - additional CGD cities
    ("Karimnagar",      "Telangana",    "18.40,79.10,18.60,79.30"),
    ("Khammam",         "Telangana",    "17.20,80.05,17.40,80.25"),
    ("Nalgonda",        "Telangana",    "17.00,79.25,17.20,79.45"),
    ("Mahbubnagar",     "Telangana",    "16.65,77.90,16.85,78.10"),
    ("Siddipet",        "Telangana",    "18.05,78.80,18.25,79.00"),
    ("Adilabad",        "Telangana",    "19.60,78.45,19.80,78.65"),
    ("Suryapet",        "Telangana",    "17.10,79.55,17.30,79.75"),
    ("Sangareddy",      "Telangana",    "17.60,77.85,17.80,78.05"),
    ("Miryalaguda",     "Telangana",    "16.85,79.50,17.05,79.70"),

    # Uttar Pradesh - additional CGD cities
    ("Prayagraj/Allahabad","Uttar Pradesh","25.35,81.75,25.55,82.00"),
    ("Aligarh",         "Uttar Pradesh","27.75,78.00,27.95,78.25"),
    ("Moradabad",       "Uttar Pradesh","28.75,78.70,28.95,78.95"),
    ("Saharanpur",      "Uttar Pradesh","29.90,77.45,30.10,77.65"),
    ("Gorakhpur",       "Uttar Pradesh","26.65,83.25,26.85,83.50"),
    ("Firozabad",       "Uttar Pradesh","27.10,78.35,27.30,78.55"),
    ("Shahjahanpur",    "Uttar Pradesh","27.80,79.85,28.00,80.05"),
    ("Jhansi",          "Uttar Pradesh","25.35,78.50,25.55,78.75"),
    ("Rampur",          "Uttar Pradesh","28.75,79.00,28.95,79.25"),
    ("Etawah",          "Uttar Pradesh","26.70,79.00,26.90,79.20"),
    ("Bulandshahr",     "Uttar Pradesh","28.35,77.80,28.55,78.00"),
    ("Hapur",           "Uttar Pradesh","28.65,77.70,28.85,77.95"),
    ("Amroha",          "Uttar Pradesh","28.85,78.40,29.05,78.65"),
    ("Unnao",           "Uttar Pradesh","26.50,80.45,26.70,80.65"),
    ("Jaunpur",         "Uttar Pradesh","25.65,82.60,25.85,82.80"),
    ("Mirzapur",        "Uttar Pradesh","25.10,82.55,25.30,82.75"),
    ("Orai/Jalaun",     "Uttar Pradesh","25.85,79.35,26.10,79.60"),
    ("Banda",           "Uttar Pradesh","25.40,80.30,25.60,80.50"),
    ("Sitapur",         "Uttar Pradesh","27.50,80.65,27.70,80.85"),
    ("Bahraich",        "Uttar Pradesh","27.50,81.55,27.70,81.75"),
    ("Lakhimpur Kheri", "Uttar Pradesh","27.85,80.70,28.05,80.95"),

    # West Bengal - additional CGD cities
    ("Durgapur",        "West Bengal",  "23.45,87.20,23.65,87.40"),
    ("Asansol",         "West Bengal",  "23.60,86.90,23.80,87.15"),
    ("Howrah",          "West Bengal",  "22.55,88.25,22.75,88.45"),
    ("Siliguri",        "West Bengal",  "26.65,88.30,26.85,88.50"),
    ("Haldia",          "West Bengal",  "22.00,88.00,22.20,88.20"),
    ("Bardhaman",       "West Bengal",  "23.20,87.75,23.40,87.95"),
    ("Kharagpur",       "West Bengal",  "22.25,87.25,22.45,87.50"),
    ("Kalyani/Nadia",   "West Bengal",  "22.90,88.40,23.10,88.65"),

    # Jharkhand - additional CGD cities
    ("Bokaro",          "Jharkhand",    "23.65,85.90,23.85,86.10"),
    ("Hazaribagh",      "Jharkhand",    "23.95,85.30,24.15,85.55"),
    ("Deoghar",         "Jharkhand",    "24.40,86.65,24.60,86.85"),
    ("Giridih",         "Jharkhand",    "24.05,86.25,24.25,86.50"),
    ("Dumka",           "Jharkhand",    "24.20,87.15,24.40,87.35"),

    # Bihar - additional CGD cities
    ("Gaya",            "Bihar",        "24.65,84.90,24.85,85.15"),
    ("Muzaffarpur",     "Bihar",        "26.05,85.30,26.25,85.50"),
    ("Bhagalpur",       "Bihar",        "25.20,87.00,25.40,87.25"),
    ("Darbhanga",       "Bihar",        "26.10,85.85,26.30,86.05"),
    ("Gaya",            "Bihar",        "24.70,85.00,24.90,85.20"),
    ("Purnia",          "Bihar",        "25.75,87.40,25.95,87.60"),
    ("Motihari",        "Bihar",        "26.55,84.85,26.75,85.10"),
    ("Hajipur",         "Bihar",        "25.65,85.15,25.85,85.35"),

    # Rajasthan - additional CGD cities
    ("Jodhpur",         "Rajasthan",    "26.20,72.90,26.40,73.15"),
    ("Udaipur",         "Rajasthan",    "24.50,73.60,24.70,73.80"),
    ("Ajmer",           "Rajasthan",    "26.35,74.55,26.55,74.75"),
    ("Bharatpur",       "Rajasthan",    "27.15,77.35,27.35,77.55"),
    ("Sikar",           "Rajasthan",    "27.55,75.10,27.75,75.30"),
    ("Sriganganagar",   "Rajasthan",    "29.80,73.80,30.00,74.05"),
    ("Hanumangarh",     "Rajasthan",    "29.55,74.25,29.75,74.45"),
    ("Jhunjhunu",       "Rajasthan",    "28.05,75.25,28.25,75.50"),
    ("Churu",           "Rajasthan",    "28.25,74.85,28.45,75.10"),
    ("Barmer",          "Rajasthan",    "25.70,71.35,25.90,71.55"),
    ("Nagaur",          "Rajasthan",    "27.15,73.60,27.35,73.85"),
    ("Pali",            "Rajasthan",    "25.70,73.25,25.90,73.45"),
    ("Tonk",            "Rajasthan",    "26.10,75.70,26.30,75.90"),
    ("Sawai Madhopur",  "Rajasthan",    "26.00,76.25,26.20,76.45"),
    ("Bhilwara",        "Rajasthan",    "25.30,74.55,25.50,74.75"),
    ("Dungarpur",       "Rajasthan",    "23.80,73.65,24.00,73.85"),
    ("Chittorgarh",     "Rajasthan",    "24.80,74.60,25.00,74.80"),
    ("Banswara",        "Rajasthan",    "23.50,74.40,23.70,74.60"),

    # Madhya Pradesh - additional CGD cities
    ("Gwalior",         "Madhya Pradesh","26.15,78.05,26.35,78.30"),
    ("Jabalpur",        "Madhya Pradesh","23.10,79.85,23.30,80.10"),
    ("Sagar",           "Madhya Pradesh","23.75,78.60,23.95,78.80"),
    ("Rewa",            "Madhya Pradesh","24.45,81.20,24.65,81.45"),
    ("Satna",           "Madhya Pradesh","24.50,80.75,24.70,80.95"),
    ("Chhindwara",      "Madhya Pradesh","22.00,78.85,22.20,79.10"),
    ("Ratlam",          "Madhya Pradesh","23.25,75.00,23.45,75.20"),
    ("Mandsaur",        "Madhya Pradesh","24.00,75.00,24.20,75.20"),
    ("Burhanpur",       "Madhya Pradesh","21.25,76.20,21.45,76.40"),
    ("Khandwa",         "Madhya Pradesh","21.80,76.25,22.00,76.45"),
    ("Morena",          "Madhya Pradesh","26.35,77.95,26.55,78.15"),
    ("Bhind",           "Madhya Pradesh","26.50,78.65,26.70,78.85"),
    ("Shivpuri",        "Madhya Pradesh","25.35,77.55,25.55,77.75"),
    ("Vidisha",         "Madhya Pradesh","23.40,77.70,23.60,77.90"),
    ("Katni",           "Madhya Pradesh","23.75,80.30,23.95,80.55"),

    # Odisha - additional CGD cities
    ("Bhubaneswar",     "Odisha",       "20.20,85.70,20.40,85.95"),
    ("Cuttack",         "Odisha",       "20.40,85.80,20.60,86.05"),
    ("Puri",            "Odisha",       "19.75,85.75,19.95,85.95"),
    ("Berhampur/Brahmapur","Odisha",    "19.25,84.70,19.45,84.90"),
    ("Baripada",        "Odisha",       "21.80,86.65,22.00,86.85"),
    ("Balasore",        "Odisha",       "21.40,86.85,21.60,87.10"),
    ("Brahmapur",       "Odisha",       "19.25,84.75,19.45,84.95"),
    ("Jharsuguda",      "Odisha",       "21.80,84.00,22.00,84.20"),
    ("Angul",           "Odisha",       "20.80,85.05,21.00,85.25"),

    # Chhattisgarh - additional CGD cities
    ("Raipur",          "Chhattisgarh", "21.15,81.55,21.35,81.80"),
    ("Durg/Bhilai",     "Chhattisgarh", "21.15,81.20,21.35,81.45"),
    ("Bilaspur CG",     "Chhattisgarh", "22.00,82.00,22.20,82.20"),
    ("Raigarh CG",      "Chhattisgarh", "21.85,83.35,22.05,83.55"),
    ("Korba",           "Chhattisgarh", "22.25,82.65,22.45,82.85"),
    ("Jagdalpur",       "Chhattisgarh", "19.05,81.95,19.25,82.15"),
    ("Rajnandgaon",     "Chhattisgarh", "20.95,81.00,21.15,81.20"),

    # Punjab - additional CGD cities
    ("Gurdaspur",       "Punjab",       "32.00,75.35,32.20,75.55"),
    ("Hoshiarpur",      "Punjab",       "31.50,75.85,31.70,76.05"),
    ("Kapurthala",      "Punjab",       "31.35,75.35,31.55,75.55"),
    ("Sangrur",         "Punjab",       "30.20,75.80,30.40,76.00"),
    ("Phagwara",        "Punjab",       "31.20,75.70,31.40,75.90"),
    ("Nawanshahr/SBS Nagar","Punjab",   "31.05,76.05,31.25,76.25"),
    ("Mansa",           "Punjab",       "29.90,75.35,30.10,75.55"),
    ("Ferozepur",       "Punjab",       "30.85,74.55,31.05,74.80"),
    ("Faridkot",        "Punjab",       "30.60,74.70,30.80,74.90"),
    ("Muktsar",         "Punjab",       "30.45,74.50,30.65,74.70"),
    ("Sri Muktsar Sahib","Punjab",      "30.40,74.40,30.60,74.60"),
    ("Fazilka",         "Punjab",       "30.35,74.00,30.55,74.20"),

    # Haryana - additional CGD cities
    ("Jhajjar",         "Haryana",      "28.55,76.55,28.75,76.75"),
    ("Kurukshetra",     "Haryana",      "29.90,76.70,30.10,76.90"),
    ("Bhiwani",         "Haryana",      "28.75,76.05,28.95,76.25"),
    ("Sirsa",           "Haryana",      "29.50,75.00,29.70,75.25"),
    ("Jind",            "Haryana",      "29.30,76.25,29.50,76.45"),
    ("Fatehabad",       "Haryana",      "29.50,75.40,29.70,75.60"),
    ("Nuh/Mewat",       "Haryana",      "28.05,77.05,28.25,77.30"),
    ("Palwal",          "Haryana",      "28.10,77.25,28.30,77.45"),
    ("Narnaul",         "Haryana",      "28.00,76.05,28.20,76.25"),
    ("Kaithal",         "Haryana",      "29.75,76.35,29.95,76.55"),
    ("Panchkula",       "Haryana",      "30.65,76.75,30.85,76.95"),

    # Uttarakhand - additional CGD cities
    ("Rishikesh",       "Uttarakhand",  "30.00,78.25,30.20,78.45"),
    ("Haldwani/Nainital","Uttarakhand", "29.15,79.45,29.35,79.65"),
    ("Rudrapur/Kashipur","Uttarakhand", "28.95,79.35,29.15,79.55"),
    ("Kotdwar",         "Uttarakhand",  "29.70,78.45,29.90,78.65"),

    # Himachal Pradesh - additional CGD cities
    ("Shimla",          "Himachal Pradesh","31.05,77.10,31.25,77.30"),
    ("Solan",           "Himachal Pradesh","30.85,77.00,31.05,77.20"),
    ("Mandi",           "Himachal Pradesh","31.65,76.85,31.85,77.10"),
    ("Baddi/Barotiwala","Himachal Pradesh","30.90,76.70,31.10,76.90"),

    # Gujarat - additional CGD cities
    ("Surendranagar",   "Gujarat",      "22.65,71.55,22.85,71.80"),
    ("Porbandar",       "Gujarat",      "21.55,69.55,21.75,69.75"),
    ("Patan",           "Gujarat",      "23.75,72.05,23.95,72.25"),
    ("Dwarka",          "Gujarat",      "22.20,68.90,22.40,69.10"),
    ("Saurashtra",      "Gujarat",      "21.95,70.45,22.15,70.65"),
    ("Ankleshwar",      "Gujarat",      "21.55,72.90,21.75,73.10"),
    ("Godhra",          "Gujarat",      "22.70,73.55,22.90,73.80"),
    ("Dahod",           "Gujarat",      "22.80,74.20,23.00,74.40"),
    ("Himmatnagar",     "Gujarat",      "23.55,72.90,23.75,73.10"),
    ("Palanpur",        "Gujarat",      "24.15,72.40,24.35,72.60"),

    # Assam - additional CGD cities
    ("Jorhat",          "Assam",        "26.70,94.15,26.90,94.40"),
    ("Silchar",         "Assam",        "24.75,92.75,24.95,92.95"),
    ("Nagaon",          "Assam",        "26.30,92.60,26.50,92.80"),
    ("Tezpur",          "Assam",        "26.60,92.75,26.80,93.00"),
    ("Lakhimpur",       "Assam",        "27.15,94.05,27.35,94.25"),
    ("Bongaigaon",      "Assam",        "26.45,90.50,26.65,90.70"),
    ("Goalpara",        "Assam",        "26.10,90.50,26.30,90.70"),
    ("Karimganj",       "Assam",        "24.75,92.35,24.95,92.55"),

    # Additional Northern/Central cities
    ("Jammu",           "Jammu and Kashmir","32.65,74.75,32.85,74.95"),
    ("Srinagar",        "Jammu and Kashmir","34.00,74.75,34.20,74.95"),
    ("Amritsar extended","Punjab",      "31.55,74.80,31.90,75.20"),
    ("Shimla ext",      "Himachal Pradesh","31.00,77.10,31.30,77.35"),

    # NE States
    ("Imphal",          "Manipur",      "24.75,93.85,24.95,94.05"),
    ("Shillong",        "Meghalaya",    "25.55,91.80,25.75,92.00"),
    ("Aizawl",          "Mizoram",      "23.65,92.65,23.85,92.85"),
    ("Kohima",          "Nagaland",     "25.60,94.05,25.80,94.25"),

    # More South India
    ("Pondicherry ext", "Tamil Nadu",   "11.75,79.80,12.00,80.00"),
    ("Kolar",           "Karnataka",    "13.10,78.05,13.30,78.25"),
    ("Chikkamagaluru",  "Karnataka",    "13.25,75.65,13.45,75.85"),
    ("Koppal",          "Karnataka",    "15.25,76.05,15.45,76.25"),
    ("Yadgir",          "Karnataka",    "16.70,77.05,16.90,77.25"),
    ("Bijapur/Vijayapura","Karnataka",  "16.80,75.65,17.00,75.85"),

    # More coastal areas
    ("Raipur/Silvassa", "Dadra and Nagar Haveli","20.25,72.95,20.45,73.15"),
    ("Panaji/Goa",      "Goa",          "15.40,73.75,15.60,73.95"),
    ("South Goa",       "Goa",          "15.10,73.90,15.30,74.10"),

    # More MP cities
    ("Balaghat",        "Madhya Pradesh","21.75,80.15,21.95,80.35"),
    ("Shahdol",         "Madhya Pradesh","23.25,81.30,23.45,81.55"),
    ("Seoni",           "Madhya Pradesh","22.05,79.45,22.25,79.65"),
    ("Mandla",          "Madhya Pradesh","22.50,80.35,22.70,80.55"),
    ("Harda",           "Madhya Pradesh","22.30,77.00,22.50,77.20"),
    ("Raisen",          "Madhya Pradesh","23.15,77.60,23.35,77.85"),
    ("Narsimhapur",     "Madhya Pradesh","22.85,79.10,23.05,79.30"),
    ("Tikamgarh",       "Madhya Pradesh","24.65,78.75,24.85,78.95"),
    ("Damoh",           "Madhya Pradesh","23.80,79.40,24.00,79.60"),
    ("Panna",           "Madhya Pradesh","24.65,80.10,24.85,80.30"),
    ("Sehore",          "Madhya Pradesh","23.15,77.00,23.35,77.20"),
    ("Hoshangabad",     "Madhya Pradesh","22.65,77.65,22.85,77.85"),
]

_OVERPASS_ENDPOINTS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://lz4.overpass-api.de/api/interpreter",
]


def _overpass(query: str, ep_idx: int = 0) -> list[dict]:
    ep = _OVERPASS_ENDPOINTS[ep_idx % len(_OVERPASS_ENDPOINTS)]
    data = urllib.parse.urlencode({"data": query})
    try:
        req = urllib.request.Request(
            ep, data=data.encode(),
            headers={"User-Agent": "CNG-Planner-India/1.0",
                     "Content-Type": "application/x-www-form-urlencoded"},
        )
        with urllib.request.urlopen(req, timeout=60) as r:
            return json.loads(r.read()).get("elements", [])
    except Exception as exc:
        print(f"  [CGD] Overpass error ({ep}): {exc}")
        if ep_idx < len(_OVERPASS_ENDPOINTS) - 1:
            time.sleep(5)
            return _overpass(query, ep_idx + 1)
        return []


def _area_stations(name: str, state: str, bbox: str) -> list[dict]:
    query = (
        f"[out:json][timeout:45];"
        f"("
        f"node[\"amenity\"=\"fuel\"]({bbox});"
        f"way[\"amenity\"=\"fuel\"]({bbox});"
        f");"
        f"out center tags;"
    )
    elements = _overpass(query)
    records: list[dict] = []
    for e in elements:
        tags = e.get("tags", {})
        # Skip stations explicitly marked as no CNG
        if tags.get("fuel:cng") == "no":
            continue
        lat = e.get("lat") or (e.get("center") or {}).get("lat")
        lon = e.get("lon") or (e.get("center") or {}).get("lon")
        if not lat or not lon:
            continue
        lat, lon = float(lat), float(lon)
        if not (6.5 <= lat <= 37.5 and 68.0 <= lon <= 97.5):
            continue
        actual_state = state_from_coords(lat, lon) or state
        if actual_state not in INDIA_STATES:
            continue
        stn_name = normalise_name(
            tags.get("name") or tags.get("operator") or tags.get("brand") or ""
        ) or f"CNG Station ({name})"
        city = (
            tags.get("addr:city") or tags.get("addr:district")
            or tags.get("addr:town") or name
        )
        records.append({
            "name": stn_name,
            "latitude": round(lat, 6),
            "longitude": round(lon, 6),
            "city": city,
            "state": actual_state,
            "source": SOURCE,
        })
    return records


def main() -> int:
    print(f"[CGD-CITIES] Fetching all fuel stations in {len(_CGD_AREAS)} CGD areas…")
    all_records: list[dict] = []

    for i, (name, state, bbox) in enumerate(_CGD_AREAS):
        print(f"  [{i+1}/{len(_CGD_AREAS)}] {name} ({state})…")
        records = _area_stations(name, state, bbox)
        print(f"    → {len(records)} fuel stations")
        all_records.extend(records)
        time.sleep(2)

    # Deduplicate within 100m at this stage
    seen: list[tuple[float, float]] = []
    unique: list[dict] = []
    for r in all_records:
        lat, lon = r["latitude"], r["longitude"]
        if not any(haversine_m(lat, lon, la, lo) < 100 for la, lo in seen):
            unique.append(r)
            seen.append((lat, lon))

    print(f"[CGD-CITIES] Total unique fuel stations in CGD areas: {len(unique)}")
    save_raw(SOURCE, unique)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
