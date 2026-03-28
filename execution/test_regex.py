import sys, os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from fill_creative_tests import extract_ad_name_from_campaign

# Test the regex against the exact campaign name format
c = "[LOTTOV7]CA6.DIANA TC238 ABO 1-50-1 - LT801.30"
print(extract_ad_name_from_campaign(c))
c2 = "[LOTTOV7]CA6.DIANA TC240 CBO 1-50-1 - LT1077"
print(extract_ad_name_from_campaign(c2))
