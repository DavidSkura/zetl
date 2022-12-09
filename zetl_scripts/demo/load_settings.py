"""
  Dave Skura
  
  File Description:
"""
settings_file = 'zetl_scripts\\demo\\demo_settings.ini'
print (" Loading settings ") # 
f = open(settings_file,'r')
ini = f.read()
print(ini)
f.close()