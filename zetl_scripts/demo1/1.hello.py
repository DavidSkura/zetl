"""
  Dave Skura
  
  File Description:
"""
import sys 

try:
	print(" Hello from Python ") # 
except Exception as e:
	print(str(e))
	sys.exit(1)

sys.exit(0)
