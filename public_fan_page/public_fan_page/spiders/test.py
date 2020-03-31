def flatten(item):
    if isinstance(item, list):
        for item_ in item:
            yield from flatten(item_)

    elif isinstance(item, dict):
        for key, value in item.items():
            yield from flatten(value)

    else:
        yield item


def p_dict(item: dict):
    from pprint import pprint

    for key, value in item.items():
        pprint(key)
        input()
        pprint(value)
        pprint("=" * 20)
        input()


def predisplay_filter(update: dict):
    items = update["jsmods"]["pre_display_requires"]
    for item in items:
        if (item[0] == "RelayPrefetchedStreamCache"
                and item[1] == "next" and True and True and True and True and True and True and True and True and True and True and True and True and True and True):
            data = item[3][1]["__bbox"]["result"]["data"]["feedback"]
            yield {
                "url": data["url"],
                "share_count": data["share_count"]["count"],
                "comment_count": data["comment_count"]["total_count"],
                "reaction_count": data["reaction_count"]["count"],
            }


def postid_parse(url):
    return urlparse(url).path
