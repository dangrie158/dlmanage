from textual.widgets import ScrollView


class JumpableScrollView(ScrollView):
    """
    A simple child class of the standart scroll view just to be able to override
    the annoying animation when scrolling to a position. the parent already has a
    `fluid` parameter that is unused, so we use it here to ignore animations when
    it is false
    """

    def scroll_to_center(self, line: int) -> None:
        self.target_y = line - self.size.height // 2
        # respect the self.fluid flag from the parent class
        if abs(self.target_y - self.y) > 1 and self.fluid:  # type: ignore
            self.animate("y", self.target_y, easing="out_cubic")
        else:
            self.y = self.target_y
