from rest_framework_xml.renderers import XMLRenderer


class CusXMLRenderer(XMLRenderer):
    def __init__(self, root_tag_name: str = 'root', item_tag_name: str = "list-item"):
        self.root_tag_name = root_tag_name
        self.item_tag_name = item_tag_name
