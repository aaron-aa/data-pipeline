# https://github.com/appannie/aa/blob/master/estimation/vans/README.md
# must be no default value

INTERFACE = {
    "operations": {
        "insert": {
            "partition_fields": ["data_granularity", "date", "market_code", "device_code", "country_code"],
            "volume": "mb",
            "retention": "-1",
            "schema": {
                "namespace": "ss.featured_story.v1.insert",
                "type": "record",
                "name": "FeaturedStory",
                "fields": [
                    {"name": "data_name", "type": "string", "default": "featured_story"},
                    {"name": "data_granularity", "type": {"type": "enum",
                                                          "name": "dgranularity", "symbols": ["daily"]}},
                    {"name": "device_code", "default": "iphone",
                     "type": {"type": "enum", "name": "device", "symbols": ["iphone", "ipad"]}},
                    {"name": "market_code", "default": "apple-store",
                     "type": {"type": "enum", "name": "market", "symbols": ["apple-store"]}},
                    {"name": "country_code", "type": "string"},
                    {"name": "date", "type": "string"},
                    {"name": "featured_story_id", "type": "long"},
                    {"name": "url", "type": "string"},
                    {"name": "rank", "type": "int"},
                    {"name": "svg", "type": "string"},
                    {"name": "raw_data", "type": "string"},
                    {"name": "creative_urls", "type": ["null", {"type": "array", "items": "string"}]},
                    {"name": "product_ids", "type": ["null", {"type": "array", "items": "long"}]},
                    {"name": "label", "type": ["null", "string"]},
                    {"name": "head", "type": ["null", "string"]},
                    {"name": "description", "type": ["null", "string"]},
                    {"name": "content", "type": ["null", "string"]},
                    {"name": "display_style", "type": ["null", {"type": "map", "values": "string"}]}
                ]
            }
        },
        "delete": {
            "partition_fields": ["data_granularity", "date", "market_code", "device_code", "country_code"],
            "schema": {
                "namespace": "ss.featured_story.v1.delete",
                "type": "record",
                "name": "FeaturedStory",
                "fields": [
                    {"name": "data_name", "type": "string", "default": "featured_story"},
                    {"name": "data_granularity", "type": {"type": "enum",
                                                          "name": "dgranularity", "symbols": ["daily"]}},
                    {"name": "device_code", "default": "iphone",
                     "type": {"type": "enum", "name": "device", "symbols": ["iphone", "ipad"]}},
                    {"name": "market_code", "default": "apple-store",
                     "type": {"type": "enum", "name": "market", "symbols": ["apple-store"]}},
                    {"name": "date", "type": "string"},
                    {"name": "country_code", "type": "string"},
                    {"name": "featured_story_id", "type": "long"}
                ]
            }
        }
    }
}


# you could provide records here for test
DUMMY_DATA = [{
    "data_name": "featured_story",
    "data_granularity": "daily",
    "market_code": "apple-store",
    "device_code": "iphone",
    "country_code": u"US",
    "date": "2017-11-31",
    "featured_story_id": 1298971998,
    "url": u"https://itunes.apple.com/cn/story/id1298971998",
    "rank": 1,
    "product_ids": [438475985],
    "raw_data": "",
    "svg": "demo svg"
}, {
    "data_name": "featured_story",
    "data_granularity": "daily",
    "market_code": "apple-store",
    "device_code": "iphone",
    "country_code": u"JP",
    "date": "2017-11-31",
    "featured_story_id": 1298971915,
    "url": u"https://itunes.apple.com/cn/story/id1298971915",
    "rank": 1,
    "product_ids": [438475005],
    "svg": "demo svg",
    "raw_data": u'{"storePlatformData":{"webexp-product":{"results":{"1298971915":{"label":"APP\\nOF THE\\nDAY","id":"1298971915","cardIds":["438475005"],"relatedContent":{"438475005":{"artwork":{"width":1024,"url":"https://is3-ssl.mzstatic.com/image/thumb/Purple128/v4/04/29/70/04297009-8248-6659-b64d-4af8d10ba38b/mzl.jvqtzfef.png/{w}x{h}bb.{f}","height":1024,"textColor3":"ebced1","textColor2":"ffffff","textColor4":"ebced1","hasAlpha":false,"textColor1":"ffffff","bgColor":"a00d1c","hasP3":false,"supportsLayeredImage":false},"artistName":"ABBYY","url":"https://itunes.apple.com/cn/app/textgrabber-6-real-time-ocr/id438475005?l=en&mt=8","id":"438475005","releaseDate":"2011-06-02","contentRatingsBySystem":{"appsApple":{"name":"4+","value":100,"rank":1}},"name":"TextGrabber 6 \\u2013 Real-Time OCR","artistUrl":"https://itunes.apple.com/cn/developer/abbyy/id347345477?l=en&mt=8","kind":"iosSoftware","copyright":"\xa9 2017, ABBYY Production LLC.","artistId":"347345477","genres":[{"genreId":"6007","name":"Productivity","url":"https://itunes.apple.com/cn/genre/id6007?l=en","mediaType":"8"},{"genreId":"6000","name":"Business","url":"https://itunes.apple.com/cn/genre/id6000?l=en","mediaType":"8"}],"description":{"standard":"ABBYY TextGrabber easily and quickly digitizes fragments of printed text, reads QR codes and turnes the recognized result into actions: call, write, translate into 100+ languages, search in the Internet or on maps, create events on the calendar, edit, voice and share in any convenient way.\\nWhen you point a camera at printed text, TextGrabber instantly captures information and recognizes it without connecting to the Internet. A unique real-time recognition mode extracts information in 60+ languages not only from documents, but from any surfaces.\\nTranslation is available as a separate in-app purchase.\\n\\n***** Winner of SUPERSTAR Award in the \\"Mobile Productivity App\\", \\"Mobile Image Capture App\\" and \\u201cText Input\\u201d categories in Mobile Star Awards\\n------------------------ \\n\\u201cTextGrabber is probably the best app which adds another function to your iPhone: a scanner\\u201d- The Irish Times\\n\\u201cThe results get delivered relatively fast, which is great. A must have for students\\u201d - appadvice.com\\n\\u201cThe Best Image-to-Text App for iPhone\\u201d - lifehacker.com \\n------------------------ \\nKEY FEATURES: \\n\\u2022 Innovative Real-Time Recognition mode based on ABBYY RTR SDK technology will digitize printed text directly on the camera screen without taking photographs.\\n\\u2022 Text recognition in 60+ languages, including Russian, English, German, Spanish, Greek, Turkish, Chinese and Korean, without an Internet connection.\\n\\u2022 All links, phone numbers, e-mail addresses, postal addresses and dates after digitization become clickable: you can click on the link, phone, write email, find the address on the maps or add an event to the calendar.\\n\\u2022  Full-text translation into 100+ languages (Internet connection is required, separate in-app purchase).\\n\\u2022 QR code reader.\\n\\u2022 Powerful text-to-speech capability with the VoiceOver system feature.\\n\\u2022 Adjustable font sizes and audio prompts to assist visually impaired people: you can increase the font size and use sound prompts to interface elements.\\n\\u2022 Share the results to any app installed on the device via the system menu.\\n\\u2022 All extracted text is automatically backed up and can be easily found in the \\u201cHistory\\u201d folder \\n------------------------ \\nWith ABBYY TextGrabber you can save and translate any printed text you need with a tap of your screen: \\n\\u2022 Texts from TV screen or smartphone\\n\\u2022 Receipts \\n\\u2022 Labels and counters\\n\\u2022 Travel documents\\n\\u2022 Magazine articles and book fragments\\n\\u2022 Manuals and instructions \\n\\u2022 Recipe ingredients, etc. \\n------------------------ \\nOCR HINT: Please select the appropriate language (up to three at a time) before recognition \\n------------------------ \\nTwitter: @ABBYY_Mobile \\nFacebook: fb.com/AbbyyMobile\\nVK: vk.com/abbyylingvo\\nYouTube: youtube.com/c/AbbyyMobile\\n----------------------- \\nABBYY TEXTGRABBER IS THE FASTEST WAY TO DIGITIZE, TRANSLATE AND ACTION ANY PRINTED INFORMATION!\\n\\nPlease leave a review if you like ABBYY TextGrabber . Thank you!"},"popularity":0.5,"offers":[{"actionText":{"short":"Buy","medium":"Buy","long":"Buy App","downloaded":"Installed","downloading":"Installing"},"type":"buy","priceFormatted":"\xa518.00","price":18,"buyParams":"productType=C&price=18000&salableAdamId=438475005&pricingParameters=STDQ&pg=default&appExtVrsId=824247157","version":{"display":"6.5","externalId":824247157},"assets":[{"flavor":"iosSoftware","size":92958720}]}]}},"editorialArtwork":{"dayCard":{"width":3524,"url":"https://is5-ssl.mzstatic.com/image/thumb/Features118/v4/04/30/34/043034e6-b0c5-15bc-5366-e6498d896b21/source/{w}x{h}{c}.{f}","height":2160,"textColor3":"193330","textColor2":"161616","textColor4":"193330","hasAlpha":false,"textColor1":"161616","bgColor":"24a79a","hasP3":false,"supportsLayeredImage":false}},"kind":"editorialItem","link":{"url":"https://itunes.apple.com/cn/story/id1298971915?l=en","kindId":"59"},"displayStyle":"Branded","cardDisplayStyle":"AppOfTheDay","popularity":0,"displaySubStyle":"AppOfDay"}},"version":2,"isAuthenticated":false,"meta":{"storefront":{"id":"143465","cc":"CN"},"language":{"tag":"en-gb"}}}},"pageData":{"componentName":"editorial_item_product_page","metricsBase":{"pageType":"editorialItem","pageId":"1298971915","pageDetails":null,"page":"editorialItem_1298971915","serverInstance":"3010005","storeFrontHeader":"","language":"2","platformId":"8","platformName":"ItunesPreview","storeFront":"143465","environmentDataCenter":"MR22"},"metrics":{"config":{},"fields":{}},"id":"1298971915","localizations":{"WEA.Error.Generic.Title":"Connecting to Apple\xa0Music","WEA.Common.SeparatorDuration":"@@hours@@, @@minutes@@","WEA.Common.Close":"Close","WEA.Common.Meta.FB.siteName.AM":"Apple Music","WEA.Common.Meta.Twitter.domain.iTunes":"iTunes","WEA.Common.TrackList.Artist":"ARTIST","WEA.Common.Hours.one":"1 Hour","WEA.Error.NativeMissing.Other.iTunes":"Get iTunes on iOS, Android, Mac and Windows","WEA.EditorialItemProductPages.CTA.Text":"This story can only be viewed in the App Store on iOS 11 with your iPhone or iPad.","WEA.Common.TrackList.Price":"PRICE","WEA.EditorialItemProductPages.Social.title.AOTD":"App of the Day: @@appName@@","WEA.Common.Ratings.other":"@@count@@ Ratings","WEA.Common.SeeAll.Title.Generic":"@@parentName@@ - @@sectionTitle@@","WEA.EditorialItemProductPages.Social.title.IAP":"In-App Purchase: @@appName@@","WEA.Common.Minutes.one":"1 Minute","WEA.Common.SeeAll.Title.Item":"@@itemName@@ - @@productName@@ - @@sectionTitle@@","WEA.Error.Generic.Meta.PageTitle":"Connecting to Apple\xa0Music.","WEA.Error.Generic.Subtitle":"If you do not have iTunes, @@downloadLink@@. If you have iTunes and it does not open automatically, try opening it from your dock or Windows task bar.","WEA.Common.Hours.other":"@@count@@ Hours","WEA.Common.DateFormat.AX":"LL","WEA.EditorialItemProductPages.Meta.PageMetaKeywords.Collection.Three":"@@storyTitle@@, @@featuredAppName1@@, @@featuredAppName2@@, @@featuredAppName3@@, @@applicationCategory@@, ios apps, app, appstore, app store, iphone, ipad, ipod touch, itouch, itunes","WEA.EditorialItemProductPages.Twitter.domain.iosSoftware":"AppStore","WEA.Common.SeparatorGeneric":"@@string1@@, @@string2@@","WEA.EditorialItemProductPages.Meta.PageMetaDescription.Collection.Two":"Learn about collection @@storyTitle@@ featuring @@featuredAppName1@@, @@featuredAppName2@@ and many more on App Store. Enjoy these apps on your iPhone, iPad and iPod touch.","WEA.Common.Free":"Free","WEA.Common.FileSize.MB":"@@count@@ MB","WEA.Common.FileSize.byte.one":"1 byte","WEA.Common.Meta.Twitter.site.AM":"@appleMusic","WEA.Common.Meta.FB.siteName.iTunes":"iTunes","WEA.Common.FileSize.KB.AX.other":"@@count@@ kilobytes","WEA.Error.Generic.Subtitle.DownloadLink.Text":"download it for free","WEA.LocalNav.CTA.FreeTrial":"Try It Now","WEA.Common.Released":"Released: @@releaseDate@@","WEA.LocalNav.Title.AppStore":"App Store Preview","WEA.Common.Ratings.one":"1 Rating","WEA.EditorialItemProductPages.CTA.Headline":"Get the Full Experience","WEA.EditorialItemProductPages.FB.siteName.iosSoftware":"App\xa0Store","WEA.LocalNav.CTA.DownloadiTunes.url":"https://www.apple.com/itunes/download/","WEA.Error.NativeMissing.iTunes.Title":"We could not find iTunes on your computer.","WEA.LocalNav.Title.iBooks":"iBooks Store Preview","WEA.Error.NativeMissing.iTunes.Download.text":"Download iTunes","WEA.LocalNav.Title.AM":"Apple\xa0Music Preview","WEA.Error.NativeMissing.iTunes.Download.link":"https://www.apple.com/itunes/download/","WEA.Common.Yes":"Yes","WEA.Error.NativeMissing.Other.AM":"Get Apple Music on iOS, Android, Mac and Windows","WEA.Common.FileSize.MB.AX.one":"1 megabyte","WEA.Error.NativeMissing.AM.Subtitle":"You need iTunes to use Apple Music","WEA.EditorialItemProductPages.Social.title.GOTD":"Game of the Day: @@appName@@","WEA.Common.More":"more","WEA.Common.SeeAll.Title.Product":"@@artistName@@ \\u2014 @@sectionTitle@@","WEA.EditorialItemProductPages.Meta.PageMetaKeywords.Collection.Two":"@@storyTitle@@, @@featuredAppName1@@, @@featuredAppName2@@, @@applicationCategory@@, ios apps, app, appstore, app store, iphone, ipad, ipod touch, itouch, itunes","WEA.LocalNav.Title.iTunes":"iTunes Preview","WEA.Common.Minutes.other":"@@count@@ Minutes","WEA.Common.FileSize.KB.AX.one":"1 kilobyte","WEA.LocalNav.Title.Podcasts":"Podcast Preview","WEA.Common.Meta.Twitter.domain.AM":"Apple Music","WEA.Common.SeeAll.Button":"See All","WEA.Common.FileSize.GB":"@@count@@ GB","WEA.EditorialItemProductPages.Meta.PageMetaDescription.Collection.Three":"Learn about collection @@storyTitle@@ featuring @@featuredAppName1@@, @@featuredAppName2@@ and @@featuredAppName3@@ on App Store. Enjoy these apps on your iPhone, iPad and iPod touch.","WEA.Common.Ratings.zero":"No Ratings","WEA.LocalNav.CTA.DownloadiTunes":"Download iTunes","WEA.Common.FileSize.GB.AX.other":"@@count@@ gigabytes","WEA.Common.TrackList.Track":"TITLE","WEA.Common.FileSize.KB":"@@count@@ KB","WEA.Error.Generic.Meta.PageKeywords":"Apple Music","WEA.Common.FileSize.GB.AX.one":"1 gigabyte","WEA.EditorialItemProductPages.Meta.PageMetaDescription":"Learn about @@storyTitle@@ on App Store. ","WEA.Common.Meta.Twitter.site.iTunes":"@iTunes","WEA.EditorialItemProductPages.CTA.Link.Url":"https://www.apple.com/cn/ios/app-store/","WEA.EditorialItemProductPages.Meta.PageMetaDescription.Collection.One":"Learn about collection @@storyTitle@@ featuring @@featuredAppName1@@ and many more on App Store. Enjoy these apps on your iPhone, iPad and iPod touch.","WEA.Common.DateFormat":"ll","WEA.Error.NativeMissing.Other.LearnMore.link":"https://www.apple.com/itunes/download/","WEA.EditorialItemProductPages.Meta.PageMetaKeywords.Collection.One":"@@storyTitle@@, @@featuredAppName1@@, @@featuredAppName2@@, @@applicationCategory@@, ios apps, app, appstore, app store, iphone, ipad, ipod touch, itouch, itunes","WEA.Common.AverageRating":"@@rating@@ out of @@ratingTotal@@","WEA.Common.Related":"Related","WEA.EditorialItemProductPages.Meta.title":"@@storyTitle@@ : App Store Story","WEA.EditorialItemProductPages.CTA.Text.AX":"VIEW @@appName@@","WEA.EditorialItemProductPages.Twitter.site.iosSoftware":"@AppStore","WEA.Error.Generic.Meta.PageDescription":"WEA.Error.Generic.Meta.PageDescription","WEA.EditorialItemProductPages.Meta.PageMetaDescription.Collection.ManyMore":"Learn about collection @@storyTitle@@ featuring @@featuredAppName1@@, @@featuredAppName2@@, @@featuredAppName3@@ and many more on App Store. Enjoy these apps on your iPhone, iPad and iPod touch.","WEA.LocalNav.Title.MAS":"Mac App Store Preview","WEA.Common.TogglePlay":"Play/Pause","WEA.Common.FileSize.byte.other":"@@count@@ bytes","WEA.Common.LearnMore":"Learn More","WEA.Common.View":"VIEW","WEA.Error.Generic.Title.iTunes":"Connecting to the iTunes Store.","WEA.Error.Generic.Meta.PageKeywords.iTunes":"iTunes\xa0Store","WEA.LocalNav.CTA.FreeTrial.url":"https://itunes.apple.com/subscribe?app=music","WEA.EditorialItemProductPages.Meta.PageMetaKeywords":"@@storyTitle@@, @@applicationCategory@@, ios apps, app, appstore, app store, iphone, ipad, ipod touch, itouch, itunes","WEA.Error.Generic.Subtitle.DownloadLink.URL":"http://www.apple.com/uk/itunes/download/","WEA.Common.FileSize.MB.AX.other":"@@count@@ megabytes","WEA.Common.TrackList.Time":"TIME","WEA.Common.TrackList.Album":"ALBUM"},"context":{"storefrontId":"143465","languageCode":"en-gb","storefront":"CN"}}}'
}]
