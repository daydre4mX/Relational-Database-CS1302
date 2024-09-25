class Integer:

    def __init__(self, n):
        self.value = n

    def increment(self):
        self.value += 1

    def __str__(self):
        return str(self.value)