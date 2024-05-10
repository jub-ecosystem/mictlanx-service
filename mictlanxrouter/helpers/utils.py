
class Utils:
    @staticmethod
    def is_true_from_str(x:str):
        if x is not None:
            return x.lower() in ["true", "1", "t", "y", "yes"]
        else:
            return False