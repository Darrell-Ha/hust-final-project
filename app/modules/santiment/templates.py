from string import Template

PRICE = Template("""
    query MyQuery{
        historyPrice(
        slug: "$slug"
        from: "$start_time"
        to: "$end_time"
        interval: "$interval"
        ){
            datetime,
            marketcapUsd,
            priceUsd
            volumeUsd
        }
    }
""")