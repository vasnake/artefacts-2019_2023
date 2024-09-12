# flake8: noqa

from pprint import pformat

import pytest


class TestConverters(object):
    def test_build_features_list(self):
        # static constexpr const feature_traits_t feature_traits[]
        feature_traits = """
{"zero",                    EFeature::eZero,                    feature_cls_t::general,         feature_type_t::categorical},
{"package_id",              EFeature::ePackage,                 feature_cls_t::banner,          feature_type_t::categorical},
{"advertiser_id",           EFeature::eAdvertiser,              feature_cls_t::banner,          feature_type_t::categorical},
{"campaign_id",             EFeature::eCampaign,                feature_cls_t::banner,          feature_type_t::categorical},
{"banner",                  EFeature::eBanner,                  feature_cls_t::banner,          feature_type_t::categorical},
{"pad_id",                  EFeature::ePad,                     feature_cls_t::publisher,       feature_type_t::categorical},
{"age_period",              EFeature::eAgePeriod,               feature_cls_t::user,            feature_type_t::categorical},
{"sex",                     EFeature::eSex,                     feature_cls_t::user,            feature_type_t::categorical},
{"geo",                     EFeature::eGeo,                     feature_cls_t::user,            feature_type_t::categorical},
{"banner_title_hash",       EFeature::eBannerTitle,             feature_cls_t::banner,          feature_type_t::categorical},
{"banner_text_hash",        EFeature::eBannerText,              feature_cls_t::banner,          feature_type_t::categorical},
{"image_id",                EFeature::eImage,                   feature_cls_t::banner,          feature_type_t::categorical},
{"user_app_install_dot",    EFeature::eUseAppInstallDot,        feature_cls_t::banner_user,     feature_type_t::numerical},
{"ssp_id",                  EFeature::eSspId,                   feature_cls_t::publisher,       feature_type_t::categorical},
{"interests",               EFeature::eInterests,               feature_cls_t::user,            feature_type_t::categorical},
{"apps",                    EFeature::eApps,                    feature_cls_t::user,            feature_type_t::categorical},
{"mid",                     EFeature::eMid,                     feature_cls_t::user,            feature_type_t::categorical},
{"mobile_osver",            EFeature::eMobileOsver,             feature_cls_t::user,            feature_type_t::categorical},
{"text_embedding",          EFeature::eTextEmbedding,           feature_cls_t::banner,          feature_type_t::categorical},
{"clicks_count",            EFeature::eClicksCount,             feature_cls_t::banner_user,     feature_type_t::categorical},
{"is_mobile",               EFeature::eIsMobile,                feature_cls_t::user,            feature_type_t::categorical},
{"pad_id_all",              EFeature::ePadAll,                  feature_cls_t::publisher,       feature_type_t::categorical},
{"appinstall_user_score",   EFeature::eAppInstallUserScore,     feature_cls_t::user,            feature_type_t::numerical},
{"app",                     EFeature::eApp,                     feature_cls_t::banner,          feature_type_t::categorical},
{"lal_appinstall_scores",   EFeature::eLalAppinstallScores,     feature_cls_t::user,            feature_type_t::numerical},
{"video_duration",          EFeature::eVDuration,               feature_cls_t::banner,          feature_type_t::categorical},
{"url_object_id_feature",   EFeature::eUrlObjectId,             feature_cls_t::banner,          feature_type_t::categorical},
{"url_object_type_feature", EFeature::eUrlObjectType,           feature_cls_t::banner,          feature_type_t::categorical},
{"contents",                EFeature::eContents,                feature_cls_t::banner,          feature_type_t::categorical},
{"user_topics",             EFeature::eUserTopics,              feature_cls_t::user,            feature_type_t::categorical},
{"banner_topics",           EFeature::eBannerTopics,            feature_cls_t::banner,          feature_type_t::categorical},
{"banner_feature_0",        EFeature::eBannerFeature0,          feature_cls_t::banner,          feature_type_t::categorical},
{"banner_feature_1",        EFeature::eBannerFeature1,          feature_cls_t::banner,          feature_type_t::categorical},
{"banner_feature_2",        EFeature::eBannerFeature2,          feature_cls_t::banner,          feature_type_t::categorical},
{"banner_feature_3",        EFeature::eBannerFeature3,          feature_cls_t::banner,          feature_type_t::categorical},
{"app_installed",           EFeature::eAppInstalled,            feature_cls_t::banner_user,     feature_type_t::categorical},
{"event_type_id",           EFeature::eEventTypeId,             feature_cls_t::event,           feature_type_t::categorical},
{"search_phrase_source",    EFeature::eSearchPhraseSource,      feature_cls_t::banner_user,     feature_type_t::categorical},
{"device_type_id",          EFeature::eDeviceType,              feature_cls_t::publisher,       feature_type_t::categorical},
{"platform_id",             EFeature::ePlatform,                feature_cls_t::publisher,       feature_type_t::categorical},
{"pad_format_id",           EFeature::ePadFormat,               feature_cls_t::publisher,       feature_type_t::categorical},
{"user_app_cats_pc_cri_wscore", EFeature::eUserAppCatsPcCriWscore, feature_cls_t::user,         feature_type_t::numerical},
{"device_id_hash",          EFeature::eDeviceIdHash,            feature_cls_t::user,            feature_type_t::categorical},
{"user_feature_0",          EFeature::eUserFeature0,            feature_cls_t::user,            feature_type_t::categorical},
{"user_feature_1",          EFeature::eUserFeature1,            feature_cls_t::user,            feature_type_t::categorical},
{"user_feature_2",          EFeature::eUserFeature2,            feature_cls_t::user,            feature_type_t::categorical},
{"user_feature_3",          EFeature::eUserFeature3,            feature_cls_t::user,            feature_type_t::categorical},
{"user_feature_4",          EFeature::eUserFeature4,            feature_cls_t::user,            feature_type_t::categorical},
{"num_banner_feature_0",    EFeature::eNumBannerFeature0,       feature_cls_t::banner,          feature_type_t::numerical},
{"num_banner_feature_1",    EFeature::eNumBannerFeature1,       feature_cls_t::banner,          feature_type_t::numerical},
{"num_banner_feature_2",    EFeature::eNumBannerFeature2,       feature_cls_t::banner,          feature_type_t::numerical},
{"num_banner_feature_3",    EFeature::eNumBannerFeature3,       feature_cls_t::banner,          feature_type_t::numerical},
{"num_banner_feature_4",    EFeature::eNumBannerFeature4,       feature_cls_t::banner,          feature_type_t::numerical},
{"num_user_feature_0",      EFeature::eNumUserFeature0,         feature_cls_t::user,          feature_type_t::numerical},
{"num_user_feature_1",      EFeature::eNumUserFeature1,         feature_cls_t::user,          feature_type_t::numerical},
{"num_user_feature_2",      EFeature::eNumUserFeature2,         feature_cls_t::user,          feature_type_t::numerical},
{"num_user_feature_3",      EFeature::eNumUserFeature3,         feature_cls_t::user,          feature_type_t::numerical},
{"num_user_feature_4",      EFeature::eNumUserFeature4,         feature_cls_t::user,          feature_type_t::numerical},
{"lal_appinstall_scores_compose", EFeature::eLalAppinstallScoresCompose, feature_cls_t::user,   feature_type_t::numerical},
{"url_shows_count",         EFeature::eUrlShowsCount,           feature_cls_t::banner_user,     feature_type_t::numerical},
{"url_clicks_count",        EFeature::eUrlClicksCount,          feature_cls_t::banner_user,     feature_type_t::numerical},
{"clicked_urls_hashes",     EFeature::eClickedUrlsHashes,       feature_cls_t::user,            feature_type_t::categorical},
{"showed_urls_hashes",      EFeature::eShowedUrlsHashes,        feature_cls_t::user,            feature_type_t::categorical},
{"url_first_show_ago",      EFeature::eUrlFirstShowAgo,         feature_cls_t::banner_user,     feature_type_t::numerical},
{"url_last_show_ago",       EFeature::eUrlLastShowAgo,          feature_cls_t::banner_user,     feature_type_t::numerical},
{"total_shows",             EFeature::eTotalShows,              feature_cls_t::user,            feature_type_t::numerical},
{"total_clicks",            EFeature::eTotalClicks,             feature_cls_t::user,            feature_type_t::numerical},
{"total_installs",          EFeature::eTotalInstalls,           feature_cls_t::user,            feature_type_t::numerical},
{"bu_feature_0",            EFeature::eBUFeature0,              feature_cls_t::banner_user,   feature_type_t::categorical},
{"bu_feature_1",            EFeature::eBUFeature1,              feature_cls_t::banner_user,   feature_type_t::categorical},
{"bu_feature_2",            EFeature::eBUFeature2,              feature_cls_t::banner_user,   feature_type_t::categorical},
{"bu_feature_3",            EFeature::eBUFeature3,              feature_cls_t::banner_user,   feature_type_t::categorical},
{"bu_feature_4",            EFeature::eBUFeature4,              feature_cls_t::banner_user,   feature_type_t::categorical},
{"num_bu_feature_0",        EFeature::eNumBUFeature0,           feature_cls_t::banner_user,   feature_type_t::numerical},
{"num_bu_feature_1",        EFeature::eNumBUFeature1,           feature_cls_t::banner_user,   feature_type_t::numerical},
{"num_bu_feature_2",        EFeature::eNumBUFeature2,           feature_cls_t::banner_user,   feature_type_t::numerical},
{"num_bu_feature_3",        EFeature::eNumBUFeature3,           feature_cls_t::banner_user,   feature_type_t::numerical},
{"num_bu_feature_4",        EFeature::eNumBUFeature4,           feature_cls_t::banner_user,   feature_type_t::numerical},
{"search_phrase_ago",       EFeature::eSearchPhraseAgo,         feature_cls_t::banner_user,          feature_type_t::numerical},
{"search_phrase_cosine_distance", EFeature::eSearchPhraseCosineDistance, feature_cls_t::banner_user, feature_type_t::numerical},
{"search_phrase_score",     EFeature::eSearchPhraseScore,       feature_cls_t::banner_user,          feature_type_t::numerical},
{"user_campaign_cosine_distance", EFeature::eUserCampaignCosineDistance, feature_cls_t::banner_user, feature_type_t::numerical},
{"rtb_publisher_id",        EFeature::eRtbPublisherId,          feature_cls_t::publisher,     feature_type_t::categorical},
{"bundle",                  EFeature::eBundle,                  feature_cls_t::publisher,     feature_type_t::categorical},
{"tagid",                   EFeature::eTagId,                   feature_cls_t::publisher,     feature_type_t::categorical},
{"content_taxons",          EFeature::eContentTaxons,           feature_cls_t::banner,        feature_type_t::categorical},
{"pattern_id",              EFeature::ePatternId,               feature_cls_t::banner,        feature_type_t::categorical},
{"banner_template_id",      EFeature::eBannerTemplateId,        feature_cls_t::publisher,     feature_type_t::categorical},
{"apps_all",                EFeature::eAppsAll,                 feature_cls_t::user,          feature_type_t::categorical},
{"url_user_ctr",            EFeature::eUrlUserCtr,              feature_cls_t::banner_user,   feature_type_t::numerical},
{"total_ctr",               EFeature::eTotalCtr,                feature_cls_t::user,          feature_type_t::numerical},
{"user_app_cats_installed", EFeature::eUserAppCatsInstalled,    feature_cls_t::user,          feature_type_t::numerical},
{"inapp_events",            EFeature::eInappEvents,             feature_cls_t::user,          feature_type_t::categorical},
{"recent_inapp_events",     EFeature::eRecentInappEvents,       feature_cls_t::user,          feature_type_t::categorical},
{"text_taxons",             EFeature::eTextTaxons,              feature_cls_t::banner,        feature_type_t::categorical},
{"num_user_feature_5",      EFeature::eNumUserFeature5,         feature_cls_t::user,          feature_type_t::numerical},
{"num_user_feature_6",      EFeature::eNumUserFeature6,         feature_cls_t::user,          feature_type_t::numerical},
{"num_user_feature_7",      EFeature::eNumUserFeature7,         feature_cls_t::user,          feature_type_t::numerical},
{"agg_banner_topic",        EFeature::eAggBannerTopic,          feature_cls_t::banner,        feature_type_t::categorical},
{"user_ctr",                EFeature::eUserCtr,                 feature_cls_t::user,          feature_type_t::numerical},
{"agg_banner_topic_user_ctr", EFeature::eAggBannerTopicUserCtr, feature_cls_t::banner_user,   feature_type_t::numerical},
        """  # name, efeature, class, type

        def clean_line(x):
            x = x.replace('{"', "").replace('"', "").replace("},", "").replace("}", "")
            return x.strip()

        lines = feature_traits.strip().split("\n")
        lines = [clean_line(x) for x in lines]
        attribs = [[y.strip() for y in x.split(",")] for x in lines]
        attribs = [
            (
                x[0],
                x[2].split("::")[1],
                x[3].split("::")[1],
            )
            for x in attribs
        ]
        class_enum = """
enum class feature_cls_t
{
    event,
    general,
    publisher,
    banner,
    user,
    banner_user,
    COUNT
};
        """
        banner_attribs = [x for x in attribs if x[1] not in {"event", "user", "banner_user"}]
        banner_attribs = sorted(
            banner_attribs,
            key=lambda x: (
                x[1],
                x[2],
                x[0],
            ),
        )
        print(pformat(banner_attribs))

        feature_type_list = {
            "num_banner_feature_0": "numerical",  # banner
        }  # ('num_banner_feature_0', 'banner', 'numerical'),
        feature_type_list = [
            """
            "{name}": "{typ}",  # {cls}
            """.format(
                name=x[0], cls=x[1], typ=x[2]
            ).strip()
            for x in banner_attribs
        ]

        print(pformat(feature_type_list))
        print(len(feature_type_list))

        print(
            """
feature_type_list = {{
{}
}}
            """.format(
                "\n".join(feature_type_list)
            )
        )


feature_type_list = {
    "advertiser_id": "categorical",  # banner
    "agg_banner_topic": "categorical",  # banner
    "app": "categorical",  # banner
    "banner": "categorical",  # banner
    "banner_feature_0": "categorical",  # banner
    "banner_feature_1": "categorical",  # banner
    "banner_feature_2": "categorical",  # banner
    "banner_feature_3": "categorical",  # banner
    "banner_text_hash": "categorical",  # banner
    "banner_title_hash": "categorical",  # banner
    "banner_topics": "categorical",  # banner
    "campaign_id": "categorical",  # banner
    "content_taxons": "categorical",  # banner
    "contents": "categorical",  # banner
    "image_id": "categorical",  # banner
    "package_id": "categorical",  # banner
    "pattern_id": "categorical",  # banner
    "text_embedding": "categorical",  # banner
    "text_taxons": "categorical",  # banner
    "url_object_id_feature": "categorical",  # banner
    "url_object_type_feature": "categorical",  # banner
    "video_duration": "categorical",  # banner
    "num_banner_feature_0": "numerical",  # banner
    "num_banner_feature_1": "numerical",  # banner
    "num_banner_feature_2": "numerical",  # banner
    "num_banner_feature_3": "numerical",  # banner
    "num_banner_feature_4": "numerical",  # banner
    "zero": "categorical",  # general
    "banner_template_id": "categorical",  # publisher
    "bundle": "categorical",  # publisher
    "device_type_id": "categorical",  # publisher
    "pad_format_id": "categorical",  # publisher
    "pad_id": "categorical",  # publisher
    "pad_id_all": "categorical",  # publisher
    "platform_id": "categorical",  # publisher
    "rtb_publisher_id": "categorical",  # publisher
    "ssp_id": "categorical",  # publisher
    "tagid": "categorical",  # publisher
}  # class not in {"event", "user", "banner_user"}
