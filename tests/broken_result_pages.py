"""A full-buffer completion bug must not pass the paged-delivery law."""
class PrematurePage:
    def __init__(self, items: int):
        if items < 1:
            raise ValueError('positive items')
        self.items = items
        self.read = 0

    # gdp: law paged done requires every row read: not result or read == items
    def poll_page(self, count: int) -> bool:
        if count < 0:
            raise ValueError('negative count')
        self.read = min(count, self.items)
        return True
