"""
  Dave Skura, Dec,2022
"""

class zetlfn:
	def f1(self,foo=''): return iter(foo.splitlines()) #  returns an iterator

	def RemoveComments(self,asql):
		foundacommentstart = 0
		foundacommentend = 0
		ret = ""

		for line in self.f1(asql):
			
			if not line.startswith( '--' ):
				if line.find('/*') > -1:
					foundacommentstart += 1

				if line.find('*/') > -1:
					foundacommentend += 1
				
				if foundacommentstart == 0:
					ret += line + '\n'

				if foundacommentstart > 0 and foundacommentend > 0:
					foundacommentstart = 0
					foundacommentend = 0	

		return ret

