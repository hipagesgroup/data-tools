from hip_data_tools.hipages.version_tracking \
    import register_class_for_version_tracking


@register_class_for_version_tracking
class Multiplier:
    def __init__(self, multiplier):
        """
        Example Class which multiplies a number
        Args:
            multiplier (int): Fixed multiplier
        """
        self.multiplier = multiplier

    def multiply_number(self, number_in):
        """
        Take a number and multipliy it
        Args:
            number_in (int): Incoming number

        Returns (int): Multiplied number

        """
        return self.multiplier*number_in
