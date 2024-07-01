class GraphSchema:
    def __init__(
        self,
        platform: str,
        interacted_with_rel: str = "INTERACTED_WITH",
        interacted_in_rel: str = "INTERACTED_IN",
        member_relation: str = "IS_MEMBER",
    ) -> None:
        """
        the graph schema

        Parameters
        ------------
        platform : str
            the name of a platform
            could be `discord`, `discourse`, `telegram`, etc
            would be converted into PascalCase
        interacted_with_rel : str
            the interacted with relation name
            default is always to be `INTERACTED_WITH`
            is always between members
        interacted_in_rel : str
            the interacted in relation name
            default is always to be `INTERACTED_IN`
            is always between a member to a platform
        member_relation : str
            the membership relation label
            default is always to be `IS_MEMBER`
        """
        platform = self._capitalize_first_letter(platform)
        self.interacted_with_rel = interacted_with_rel
        self.interacted_in_rel = interacted_in_rel
        self.member_relation = member_relation

        self.user_label = platform + "Member"
        self.platform_label = platform + "Platform"

    def _capitalize_first_letter(self, platform: str):
        if "_" in platform or " " in platform:
            raise ValueError(
                "no underline or spaces should be in platform name. "
                f"Given name: {platform}"
            )
        return platform.title()
