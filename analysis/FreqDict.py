import operator

class FreqDict:

	def __init__(self):
		self.d = {}

	def add(self, k):
		if k in self.d:
			self.d[k] += 1
		else:
			self.d[k] = 1

	def sort(self, descending = True):
		return sorted(self.d.items(), key=operator.itemgetter(1), reverse=descending)