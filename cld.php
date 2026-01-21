{
  "$schema": "https://developer.microsoft.com/json-schemas/sp/v2/view-formatting.schema.json",
  "calendarEventFormat": {
    "elmType": "div",
    "attributes": {
      "class": "=if([$Category]=='Meeting','sp-css-backgroundColor-successBackground50 sp-css-color-BlackText', if([$Category]=='Training','sp-css-backgroundColor-BgPeach sp-css-color-BlackText', if([$Category]=='Leave','sp-css-backgroundColor-BgGold sp-css-color-BlackText','sp-css-backgroundColor-neutralBackground20 sp-css-color-BlackText')))"
    },
    "children": [
      {
        "elmType": "span",
        "txtContent": "@currentField"
      }
    ]
  }
}




{
  "$schema": "https://developer.microsoft.com/json-schemas/sp/v2/view-formatting.schema.json",
  "calendarEventFormat": {
    "elmType": "div",
    "style": {
      "background-color": "=if([$Category]=='Meeting','#0078d4', if([$Category]=='Holiday','#107c10', if([$Category]=='Training','#ff8c00', if([$Category]=='Leave','#e81123','#605e5c'))))",
      "color": "white",
      "padding": "6px",
      "border-radius": "4px"
    },
    "children": [
      {
        "elmType": "span",
        "txtContent": "@currentField"
      }
    ]
  }
}
