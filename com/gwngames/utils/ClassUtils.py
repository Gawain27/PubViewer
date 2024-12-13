class ClassUtils:
    @staticmethod
    def get_all_subclasses(cls: type):
        all_subclasses = cls.__subclasses__()

        for subclass in all_subclasses:
            subclass_subclasses = ClassUtils.get_all_subclasses(subclass)
            all_subclasses.extend(subclass_subclasses)

        return all_subclasses

    @staticmethod
    def add_class_to_superclass(subclass: type, superclass: type):
        if subclass not in superclass.__subclasses__():
            # Python does not allow direct modification of __subclasses__ attribute.
            # Hence, we simulate this by manually setting the __bases__ of the subclass.
            subclass.__bases__ = (superclass,) + subclass.__bases__[1:]

    @staticmethod
    def add_subclasses_to_superclass(superclass: type, subclasses: set):
        for subclass in subclasses:
            if subclass not in superclass.__subclasses__():
                subclass.__bases__ = (superclass,) + subclass.__bases__[1:]
