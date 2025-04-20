class StarlakeSnowflakeError(Exception):
    """The base exception class for all Starlake Python related Errors."""

    def __init__(self, message: str, error_code: int = 1) -> None:
        self.__message = message
        self.__error_code = error_code
        super().__init__(self.message)

    @property
    def message(self):
        return self.__message

    @property
    def error_code(self):
        return self.__error_code

    def __str__(self):
        return f"[{self.message} (Error Code: {self.error_code})]"
