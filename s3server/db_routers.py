class MetadataRouter(object):
    """
    A router to control all database operations on models that app_label == 'metadata'
    """
    METADATA = 'metadata'
    METADATA_DB = METADATA

    PART_METADATA = 'part_metadata'
    PART_METADATA_DB = PART_METADATA

    def db_for_read(self, model, **hints):
        """
        Attempts to read metadata models go to metadata.
        """
        if model._meta.app_label == 'metadata':
            return 'metadata'
        elif model._meta.app_label == self.PART_METADATA:
            return self.PART_METADATA_DB

        return None

    def db_for_write(self, model, **hints):
        """
        Attempts to write metadata models go to metadata.
        """
        if model._meta.app_label == 'metadata':
            return 'metadata'
        elif model._meta.app_label == self.PART_METADATA:
            return self.PART_METADATA_DB

        return None

    def allow_relation(self, obj1, obj2, **hints):
        """
        Allow relations if a model is involved.
        """
        if obj1._meta.app_label == 'metadata' or obj2._meta.app_label == 'metadata':
           return True
        elif obj1._meta.app_label == self.PART_METADATA or obj2._meta.app_label == self.PART_METADATA:
           return True

        return None

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        """
        Make sure the metadata Model class only appears in the 'metadata'
        database.
        """
        if app_label == 'metadata':
            return db == 'metadata'
        elif app_label == self.PART_METADATA:
            return db == self.PART_METADATA_DB

        return None
