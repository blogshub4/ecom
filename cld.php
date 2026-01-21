{
  "$schema": "https://developer.microsoft.com/json-schemas/sp/v2/view-formatting.schema.json",
  "calendarEventFormat": {
    "elmType": "div",
    "style": {
      "color": "=if([$Category]=='Meeting','red', if([$Category]=='Training','blue', if([$Category]=='Leave','green','black')))"
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
