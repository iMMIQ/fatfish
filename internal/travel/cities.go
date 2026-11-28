package travel

import (
	"math"
	"strings"
	"time"

	"github.com/iMMIQ/fatfish/internal/model"
)

// builtinCities provides a compact, curated global atlas (~110 cities across all regions)
// with accurate coordinates, IANA timezones, currencies, IATA codes, and local flavor notes.
var builtinCities = []model.TravelCity{
	// --- East Asia ---
	{ID: "hangzhou", Name: "Hangzhou", NameZH: "杭州", Country: "China", CountryZH: "中国", Region: "east_asia", Lat: 30.2741, Lon: 120.1551, Timezone: "Asia/Shanghai", Currency: "CNY", AirportCode: "HGH", Tags: []string{"西湖", "龙井", "大厂", "夜宵"}, Vibe: "西湖边散步、滨江吹风、梅雨季湿漉漉"},
	{ID: "shanghai", Name: "Shanghai", NameZH: "上海", Country: "China", CountryZH: "中国", Region: "east_asia", Lat: 31.2304, Lon: 121.4737, Timezone: "Asia/Shanghai", Currency: "CNY", AirportCode: "PVG", Tags: []string{"咖啡", "外滩", "梧桐区"}, Vibe: "满街精品咖啡、梧桐树下遛弯、地铁人挤人"},
	{ID: "beijing", Name: "Beijing", NameZH: "北京", Country: "China", CountryZH: "中国", Region: "east_asia", Lat: 39.9042, Lon: 116.4074, Timezone: "Asia/Shanghai", Currency: "CNY", AirportCode: "PEK", Tags: []string{"胡同", "铜锅", "早高峰"}, Vibe: "胡同里钻小馆子、风大干燥、二环堵车"},
	{ID: "chengdu", Name: "Chengdu", NameZH: "成都", Country: "China", CountryZH: "中国", Region: "east_asia", Lat: 30.5728, Lon: 104.0668, Timezone: "Asia/Shanghai", Currency: "CNY", AirportCode: "TFU", Tags: []string{"火锅", "茶馆", "熊猫", "慢生活"}, Vibe: "人民公园喝盖碗茶、冒菜辣到冒汗、晒太阳摸鱼"},
	{ID: "chongqing", Name: "Chongqing", NameZH: "重庆", Country: "China", CountryZH: "中国", Region: "east_asia", Lat: 29.5630, Lon: 106.5516, Timezone: "Asia/Shanghai", Currency: "CNY", AirportCode: "CKG", Tags: []string{"8D魔幻", "小面", "轻轨"}, Vibe: "导航全面失灵、爬坡上坎、半夜吃老火锅"},
	{ID: "xian", Name: "Xi'an", NameZH: "西安", Country: "China", CountryZH: "中国", Region: "east_asia", Lat: 34.3416, Lon: 108.9398, Timezone: "Asia/Shanghai", Currency: "CNY", AirportCode: "XIY", Tags: []string{"碳水", "城墙", "肉夹馍"}, Vibe: "碳水炸弹天堂、城墙吹晚风、一碗油泼面管饱"},
	{ID: "harbin", Name: "Harbin", NameZH: "哈尔滨", Country: "China", CountryZH: "中国", Region: "east_asia", Lat: 45.8038, Lon: 126.5350, Timezone: "Asia/Shanghai", Currency: "CNY", AirportCode: "HRB", Tags: []string{"冰雪", "铁锅炖", "中央大街"}, Vibe: "零下啃马迭尔冰棍、东北菜份量大到离谱、手机冻关机"},
	{ID: "kunming", Name: "Kunming", NameZH: "昆明", Country: "China", CountryZH: "中国", Region: "east_asia", Lat: 24.8801, Lon: 102.8329, Timezone: "Asia/Shanghai", Currency: "CNY", AirportCode: "KMG", Tags: []string{"春城", "菌子", "过桥米线"}, Vibe: "紫外线强但风凉爽、菜市场逛花和野生菌、海鸥抢面包"},
	{ID: "xiamen", Name: "Xiamen", NameZH: "厦门", Country: "China", CountryZH: "中国", Region: "east_asia", Lat: 24.4798, Lon: 118.0894, Timezone: "Asia/Shanghai", Currency: "CNY", AirportCode: "XMN", Tags: []string{"海风", "沙茶面", "环岛路"}, Vibe: "骑小电驴吹海风、八市吃海鲜、沙茶面加满料"},
	{ID: "hong_kong", Name: "Hong Kong", NameZH: "香港", Country: "China", CountryZH: "中国", Region: "east_asia", Lat: 22.3193, Lon: 114.1694, Timezone: "Asia/Hong_Kong", Currency: "HKD", AirportCode: "HKG", Tags: []string{"茶餐厅", "维港", "天星小轮"}, Vibe: "冷气开到像冰库、茶餐厅老板语速拉满、旺角霓虹"},
	{ID: "taipei", Name: "Taipei", NameZH: "台北", Country: "China", CountryZH: "中国", Region: "east_asia", Lat: 25.0330, Lon: 121.5654, Timezone: "Asia/Taipei", Currency: "TWD", AirportCode: "TPE", Tags: []string{"夜市", "牛肉面", "捷运"}, Vibe: "巷口豆浆卤肉饭、午后雷阵雨、逛夜市吃到扶墙"},
	{ID: "tokyo", Name: "Tokyo", NameZH: "东京", Country: "Japan", CountryZH: "日本", Region: "east_asia", Lat: 35.6762, Lon: 139.6503, Timezone: "Asia/Tokyo", Currency: "JPY", AirportCode: "HND", Tags: []string{"便利店", "秋叶原", "山手线", "拉面"}, Vibe: "新宿站必迷路、半夜便利店买炸鸡饭团、秋叶原逛街"},
	{ID: "kyoto", Name: "Kyoto", NameZH: "京都", Country: "Japan", CountryZH: "日本", Region: "east_asia", Lat: 35.0116, Lon: 135.7681, Timezone: "Asia/Tokyo", Currency: "JPY", AirportCode: "KIX", Tags: []string{"鸭川", "抹茶", "神社"}, Vibe: "鸭川边发呆、游客多到挤不动、老巷子里找小酒馆"},
	{ID: "osaka", Name: "Osaka", NameZH: "大阪", Country: "Japan", CountryZH: "日本", Region: "east_asia", Lat: 34.6937, Lon: 135.5023, Timezone: "Asia/Tokyo", Currency: "JPY", AirportCode: "KIX", Tags: []string{"章鱼烧", "道顿堀", "串炸"}, Vibe: "一路从早吃到晚、大阪大妈声音大、梅田地下迷宫"},
	{ID: "sapporo", Name: "Sapporo", NameZH: "札幌", Country: "Japan", CountryZH: "日本", Region: "east_asia", Lat: 43.0618, Lon: 141.3545, Timezone: "Asia/Tokyo", Currency: "JPY", AirportCode: "CTS", Tags: []string{"雪国", "汤咖喱", "海鲜"}, Vibe: "路滑差点平地摔、汤咖喱配北海道牛奶、泡完温泉犯困"},
	{ID: "fukuoka", Name: "Fukuoka", NameZH: "福冈", Country: "Japan", CountryZH: "日本", Region: "east_asia", Lat: 33.5902, Lon: 130.4017, Timezone: "Asia/Tokyo", Currency: "JPY", AirportCode: "FUK", Tags: []string{"屋台", "豚骨拉面", "海港"}, Vibe: "晚上钻屋台吃拉面关东煮、机场离市区近到离谱"},
	{ID: "naha", Name: "Okinawa", NameZH: "冲绳", Country: "Japan", CountryZH: "日本", Region: "east_asia", Lat: 26.2124, Lon: 127.6809, Timezone: "Asia/Tokyo", Currency: "JPY", AirportCode: "OKA", Tags: []string{"海岛", "水族馆", "黑糖"}, Vibe: "海蓝得像滤镜、国际通逛吃、晒太阳差点变烤鱼"},
	{ID: "seoul", Name: "Seoul", NameZH: "首尔", Country: "South Korea", CountryZH: "韩国", Region: "east_asia", Lat: 37.5665, Lon: 126.9780, Timezone: "Asia/Seoul", Currency: "KRW", AirportCode: "ICN", Tags: []string{"冰美式", "烤肉", "汉江"}, Vibe: "零度也人人捧冰美式、坡路多到腿酸、深夜炸鸡配啤酒"},
	{ID: "busan", Name: "Busan", NameZH: "釜山", Country: "South Korea", CountryZH: "韩国", Region: "east_asia", Lat: 35.1796, Lon: 129.0756, Timezone: "Asia/Seoul", Currency: "KRW", AirportCode: "PUS", Tags: []string{"海云台", "猪肉汤饭", "胶囊小火车"}, Vibe: "海风吹乱头发、扎嘎其市场看海鲜、海边发呆"},
	{ID: "jeju", Name: "Jeju", NameZH: "济州岛", Country: "South Korea", CountryZH: "韩国", Region: "east_asia", Lat: 33.4996, Lon: 126.5312, Timezone: "Asia/Seoul", Currency: "KRW", AirportCode: "CJU", Tags: []string{"橘子", "黑猪肉", "海边咖啡"}, Vibe: "风大到伞直接翻面、海景咖啡馆摸鱼、橘子甜得离谱"},
	{ID: "ulaanbaatar", Name: "Ulaanbaatar", NameZH: "乌兰巴托", Country: "Mongolia", CountryZH: "蒙古", Region: "east_asia", Lat: 47.8864, Lon: 106.9057, Timezone: "Asia/Ulaanbaatar", Currency: "MNT", AirportCode: "UBN", Tags: []string{"草原", "牛羊肉", "星空"}, Vibe: "天宽云低、早晚温差大到离谱、顿顿硬核手抓肉"},

	// --- Southeast Asia ---
	{ID: "bangkok", Name: "Bangkok", NameZH: "曼谷", Country: "Thailand", CountryZH: "泰国", Region: "se_asia", Lat: 13.7563, Lon: 100.5018, Timezone: "Asia/Bangkok", Currency: "THB", AirportCode: "BKK", Tags: []string{"冬阴功", "突突车", "夜市", "711"}, Vibe: "热到融化、711热压吐司续命、马路堵到怀疑人生"},
	{ID: "chiang_mai", Name: "Chiang Mai", NameZH: "清迈", Country: "Thailand", CountryZH: "泰国", Region: "se_asia", Lat: 18.7883, Lon: 98.9853, Timezone: "Asia/Bangkok", Currency: "THB", AirportCode: "CNX", Tags: []string{"数字游民", "咖啡馆", "古城"}, Vibe: "物价感人、古城里到处是数字游民抱电脑、芒果糯米饭"},
	{ID: "phuket", Name: "Phuket", NameZH: "普吉岛", Country: "Thailand", CountryZH: "泰国", Region: "se_asia", Lat: 7.8804, Lon: 98.3923, Timezone: "Asia/Bangkok", Currency: "THB", AirportCode: "HKT", Tags: []string{"海滩", "跳岛", "日落"}, Vibe: "躺沙滩喝椰子、突突车宰客斗智斗勇、晚霞很绝"},
	{ID: "singapore", Name: "Singapore", NameZH: "新加坡", Country: "Singapore", CountryZH: "新加坡", Region: "se_asia", Lat: 1.3521, Lon: 103.8198, Timezone: "Asia/Singapore", Currency: "SGD", AirportCode: "SIN", Tags: []string{"熟食中心", "肉骨茶", "热带阵雨"}, Vibe: "出门五分钟暴汗、商场冷气冻人、食阁海南鸡饭真香"},
	{ID: "kuala_lumpur", Name: "Kuala Lumpur", NameZH: "吉隆坡", Country: "Malaysia", CountryZH: "马来西亚", Region: "se_asia", Lat: 3.1390, Lon: 101.6869, Timezone: "Asia/Kuala_Lumpur", Currency: "MYR", AirportCode: "KUL", Tags: []string{"榴莲", "双子塔", "椰浆饭"}, Vibe: "路边摊吃猫山王、午后必下太阳雨、中文无缝沟通"},
	{ID: "penang", Name: "Penang", NameZH: "槟城", Country: "Malaysia", CountryZH: "马来西亚", Region: "se_asia", Lat: 5.4141, Lon: 100.3288, Timezone: "Asia/Kuala_Lumpur", Currency: "MYR", AirportCode: "PEN", Tags: []string{"炒粿条", "骑楼", "壁画"}, Vibe: "老骑楼下吃炒粿条煎蕊、慢节奏逛老街、热并快乐着"},
	{ID: "hanoi", Name: "Hanoi", NameZH: "河内", Country: "Vietnam", CountryZH: "越南", Region: "se_asia", Lat: 21.0285, Lon: 105.8542, Timezone: "Asia/Ho_Chi_Minh", Currency: "VND", AirportCode: "HAN", Tags: []string{"河粉", "鸡蛋咖啡", "摩托车海"}, Vibe: "过马路全凭勇气、坐塑料小板凳嗦河粉、喝浓得睡不着的滴漏咖啡"},
	{ID: "ho_chi_minh", Name: "Ho Chi Minh City", NameZH: "胡志明市", Country: "Vietnam", CountryZH: "越南", Region: "se_asia", Lat: 10.8231, Lon: 106.6297, Timezone: "Asia/Ho_Chi_Minh", Currency: "VND", AirportCode: "SGN", Tags: []string{"法棍", "咖啡公寓", "摩托大军"}, Vibe: "满街法式咖啡馆、越南盾零多到数晕、路边法棍酥掉渣"},
	{ID: "bali", Name: "Bali", NameZH: "巴厘岛", Country: "Indonesia", CountryZH: "印尼", Region: "se_asia", Lat: -8.4095, Lon: 115.1889, Timezone: "Asia/Makassar", Currency: "IDR", AirportCode: "DPS", Tags: []string{"冲浪", "乌布稻田", "堵车"}, Vibe: "骑摩托堵在窄巷里、海边看冲浪、小心猴子抢眼镜"},
	{ID: "siem_reap", Name: "Siem Reap", NameZH: "暹粒", Country: "Cambodia", CountryZH: "柬埔寨", Region: "se_asia", Lat: 13.3633, Lon: 103.8564, Timezone: "Asia/Phnom_Penh", Currency: "KHR", AirportCode: "SAI", Tags: []string{"吴哥窟", "日出", "热带丛林"}, Vibe: "四点爬起来看石头日出、热到灵魂出窍、夜市喝冰沙"},
	{ID: "manila", Name: "Manila", NameZH: "马尼拉", Country: "Philippines", CountryZH: "菲律宾", Region: "se_asia", Lat: 14.5995, Lon: 120.9842, Timezone: "Asia/Manila", Currency: "PHP", AirportCode: "MNL", Tags: []string{"海湾日落", "吉普尼", "商场"}, Vibe: "堵车堪比停车场、街上五颜六色吉普尼、吹海湾晚风"},

	// --- South Asia, Central Asia & Middle East ---
	{ID: "kathmandu", Name: "Kathmandu", NameZH: "加德满都", Country: "Nepal", CountryZH: "尼泊尔", Region: "south_asia", Lat: 27.7172, Lon: 85.3240, Timezone: "Asia/Kathmandu", Currency: "NPR", AirportCode: "KTM", Tags: []string{"雪山", "泰米尔", "徒步"}, Vibe: "泰米尔街灰尘有点大、抬头就能看见雪山、吃尼泊尔饺子momo"},
	{ID: "colombo", Name: "Colombo", NameZH: "科伦坡", Country: "Sri Lanka", CountryZH: "斯里兰卡", Region: "south_asia", Lat: 6.9271, Lon: 79.8612, Timezone: "Asia/Colombo", Currency: "LKR", AirportCode: "CMB", Tags: []string{"海上小火车", "红茶", "咖喱"}, Vibe: "坐敞篷海边火车吹风、喝锡兰红茶、顿顿香料拉满"},
	{ID: "male", Name: "Male", NameZH: "马尔代夫", Country: "Maldives", CountryZH: "马尔代夫", Region: "south_asia", Lat: 4.1755, Lon: 73.5093, Timezone: "Indian/Maldives", Currency: "MVR", AirportCode: "MLE", Tags: []string{"果冻海", "浮潜", "水飞"}, Vibe: "终于回到大海老家、每天泡水里发呆、防晒霜当水涂"},
	{ID: "jaipur", Name: "Jaipur", NameZH: "斋浦尔", Country: "India", CountryZH: "印度", Region: "south_asia", Lat: 26.9124, Lon: 75.7873, Timezone: "Asia/Kolkata", Currency: "INR", AirportCode: "JAI", Tags: []string{"粉红之城", "香料奶茶", "市集"}, Vibe: "满城粉红老墙、喇叭声自带BGM、喝一口超甜玛萨拉奶茶"},
	{ID: "almaty", Name: "Almaty", NameZH: "阿拉木图", Country: "Kazakhstan", CountryZH: "哈萨克斯坦", Region: "middle_east", Lat: 43.2220, Lon: 76.8512, Timezone: "Asia/Almaty", Currency: "KZT", AirportCode: "ALA", Tags: []string{"雪山背景", "苹果城", "烤肉"}, Vibe: "城市背后一整排雪山、大巴扎买干果、咖啡馆意外很卷"},
	{ID: "samarkand", Name: "Samarkand", NameZH: "撒马尔罕", Country: "Uzbekistan", CountryZH: "乌兹别克斯坦", Region: "middle_east", Lat: 39.6542, Lon: 66.9597, Timezone: "Asia/Samarkand", Currency: "UZS", AirportCode: "SKD", Tags: []string{"丝绸之路", "蓝瓷穹顶", "手抓饭"}, Vibe: "满眼伊斯兰蓝釉砖、吃一大盘油香手抓饭、干爽大太阳"},
	{ID: "tbilisi", Name: "Tbilisi", NameZH: "第比利斯", Country: "Georgia", CountryZH: "格鲁吉亚", Region: "middle_east", Lat: 41.7151, Lon: 44.8271, Timezone: "Asia/Tbilisi", Currency: "GEL", AirportCode: "TBS", Tags: []string{"红酒", "老城阳台", "流浪狗"}, Vibe: "满街懒洋洋的大狗、红酒比矿泉水便宜、老城坡路配雕花木阳台"},
	{ID: "istanbul", Name: "Istanbul", NameZH: "伊斯坦布尔", Country: "Turkey", CountryZH: "土耳其", Region: "middle_east", Lat: 41.0082, Lon: 28.9784, Timezone: "Europe/Istanbul", Currency: "TRY", AirportCode: "IST", Tags: []string{"撸猫", "博斯普鲁斯", "烤肉"}, Vibe: "满街霸道橘猫、坐轮渡横跨欧亚、喝郁金香杯红茶"},
	{ID: "cappadocia", Name: "Cappadocia", NameZH: "卡帕多奇亚", Country: "Turkey", CountryZH: "土耳其", Region: "middle_east", Lat: 38.6431, Lon: 34.8289, Timezone: "Europe/Istanbul", Currency: "TRY", AirportCode: "NAV", Tags: []string{"热气球", "洞穴酒店", "地貌"}, Vibe: "五点冻得发抖看热气球升空、住洞穴房间信号微弱"},
	{ID: "dubai", Name: "Dubai", NameZH: "迪拜", Country: "UAE", CountryZH: "阿联酋", Region: "middle_east", Lat: 25.2048, Lon: 55.2708, Timezone: "Asia/Dubai", Currency: "AED", AirportCode: "DXB", Tags: []string{"摩天楼", "沙漠", "豪车"}, Vibe: "室外像烤箱室内像冷库、连公交站都有空调、满街土豪感"},
	{ID: "doha", Name: "Doha", NameZH: "多哈", Country: "Qatar", CountryZH: "卡塔尔", Region: "middle_east", Lat: 25.2854, Lon: 51.5310, Timezone: "Asia/Qatar", Currency: "QAR", AirportCode: "DOH", Tags: []string{"转机枢纽", "博物馆", "海湾"}, Vibe: "机场大到走断腿、瓦其夫老市场闻香料、海风热烘烘"},
	{ID: "cairo", Name: "Cairo", NameZH: "开罗", Country: "Egypt", CountryZH: "埃及", Region: "africa", Lat: 30.0444, Lon: 31.2357, Timezone: "Africa/Cairo", Currency: "EGP", AirportCode: "CAI", Tags: []string{"金字塔", "尼罗河", "砍价"}, Vibe: "和景区小贩极限拉扯、金字塔比想象中大得多、车流没有车道线"},
	{ID: "amman", Name: "Amman", NameZH: "安曼", Country: "Jordan", CountryZH: "约旦", Region: "middle_east", Lat: 31.9454, Lon: 35.9284, Timezone: "Asia/Amman", Currency: "JOD", AirportCode: "AMM", Tags: []string{"佩特拉", "死海", "鹰嘴豆泥"}, Vibe: "满城米白色石头山城、死海漂浮看报纸、烤饼蘸鹰嘴豆泥"},

	// --- Europe ---
	{ID: "london", Name: "London", NameZH: "伦敦", Country: "UK", CountryZH: "英国", Region: "europe", Lat: 51.5072, Lon: -0.1276, Timezone: "Europe/London", Currency: "GBP", AirportCode: "LHR", Tags: []string{"地铁无信号", "炸鱼薯条", "阴雨"}, Vibe: "地铁里彻底失联、一天下五次阵雨、物价贵到心在滴血"},
	{ID: "edinburgh", Name: "Edinburgh", NameZH: "爱丁堡", Country: "UK", CountryZH: "英国", Region: "europe", Lat: 55.9533, Lon: -3.1883, Timezone: "Europe/London", Currency: "GBP", AirportCode: "EDI", Tags: []string{"城堡", "风笛", "妖风"}, Vibe: "石头老城像魔法学院、海风吹到怀疑人生、进小酒馆避寒"},
	{ID: "dublin", Name: "Dublin", NameZH: "都柏林", Country: "Ireland", CountryZH: "爱尔兰", Region: "europe", Lat: 53.3498, Lon: -6.2603, Timezone: "Europe/Dublin", Currency: "EUR", AirportCode: "DUB", Tags: []string{"黑啤", "民谣酒馆", "绿岛"}, Vibe: "随便进家酒吧都有人拉小提琴、云低得像伸手就能摸到"},
	{ID: "paris", Name: "Paris", NameZH: "巴黎", Country: "France", CountryZH: "法国", Region: "europe", Lat: 48.8566, Lon: 2.3522, Timezone: "Europe/Paris", Currency: "EUR", AirportCode: "CDG", Tags: []string{"可颂", "塞纳河", "罢工", "博物馆"}, Vibe: "边啃法棍边防小偷、塞纳河边晒太阳、地铁站味道感人"},
	{ID: "nice", Name: "Nice", NameZH: "尼斯", Country: "France", CountryZH: "法国", Region: "europe", Lat: 43.7102, Lon: 7.2620, Timezone: "Europe/Paris", Currency: "EUR", AirportCode: "NCE", Tags: []string{"蔚蓝海岸", "鹅卵石滩", "阳光"}, Vibe: "地中海蓝得不讲道理、鹅卵石滩硌脚、老城吃冰淇淋"},
	{ID: "lisbon", Name: "Lisbon", NameZH: "里斯本", Country: "Portugal", CountryZH: "葡萄牙", Region: "europe", Lat: 38.7223, Lon: -9.1393, Timezone: "Europe/Lisbon", Currency: "EUR", AirportCode: "LIS", Tags: []string{"蛋挞", "黄电车", "陡坡"}, Vibe: "爬坡爬到腿抖、热蛋挞撒肉桂粉绝了、看大西洋日落"},
	{ID: "porto", Name: "Porto", NameZH: "波尔图", Country: "Portugal", CountryZH: "葡萄牙", Region: "europe", Lat: 41.1579, Lon: -8.6291, Timezone: "Europe/Lisbon", Currency: "EUR", AirportCode: "OPO", Tags: []string{"蓝白瓷砖", "波特酒", "铁桥"}, Vibe: "杜罗河畔吹晚风、满墙蓝白瓷砖画、老城阶梯绕晕人"},
	{ID: "madrid", Name: "Madrid", NameZH: "马德里", Country: "Spain", CountryZH: "西班牙", Region: "europe", Lat: 40.4168, Lon: -3.7038, Timezone: "Europe/Madrid", Currency: "EUR", AirportCode: "MAD", Tags: []string{"火腿", "午休", "夜生活"}, Vibe: "晚上九点才开始吃晚饭、下午店铺全关门午休、阳光晃眼"},
	{ID: "barcelona", Name: "Barcelona", NameZH: "巴塞罗那", Country: "Spain", CountryZH: "西班牙", Region: "europe", Lat: 41.3874, Lon: 2.1686, Timezone: "Europe/Madrid", Currency: "EUR", AirportCode: "BCN", Tags: []string{"高迪", "海鲜饭", "地中海"}, Vibe: "高迪的房子像外星建筑、拉布拉大道死死捂紧背包、吃海鲜饭"},
	{ID: "rome", Name: "Rome", NameZH: "罗马", Country: "Italy", CountryZH: "意大利", Region: "europe", Lat: 41.9028, Lon: 12.4964, Timezone: "Europe/Rome", Currency: "EUR", AirportCode: "FCO", Tags: []string{"古迹", "意面", "冰淇淋"}, Vibe: "走两步就是两千年前的废墟、石板路震手、一天吃两球Gelato"},
	{ID: "florence", Name: "Florence", NameZH: "佛罗伦萨", Country: "Italy", CountryZH: "意大利", Region: "europe", Lat: 43.7696, Lon: 11.2558, Timezone: "Europe/Rome", Currency: "EUR", AirportCode: "FLR", Tags: []string{"牛排", "文艺复兴", "老桥"}, Vibe: "一大块T骨牛排吃到撑、满街艺术品、黄昏在河边吹风"},
	{ID: "venice", Name: "Venice", NameZH: "威尼斯", Country: "Italy", CountryZH: "意大利", Region: "europe", Lat: 45.4408, Lon: 12.3155, Timezone: "Europe/Rome", Currency: "EUR", AirportCode: "VCE", Tags: []string{"水城", "小巷迷路", "水上巴士"}, Vibe: "小巷窄到地图飘移、坐水上巴士晃悠、作为鲸鱼在这里很自在"},
	{ID: "milan", Name: "Milan", NameZH: "米兰", Country: "Italy", CountryZH: "意大利", Region: "europe", Lat: 45.4642, Lon: 9.1900, Timezone: "Europe/Rome", Currency: "EUR", AirportCode: "MXP", Tags: []string{"大教堂", "浓缩咖啡", "电车"}, Vibe: "站吧台一口闷意式浓缩、老式木头电车叮当响、路人穿搭很卷"},
	{ID: "zurich", Name: "Zurich", NameZH: "苏黎世", Country: "Switzerland", CountryZH: "瑞士", Region: "europe", Lat: 47.3769, Lon: 8.5417, Timezone: "Europe/Zurich", Currency: "CHF", AirportCode: "ZRH", Tags: []string{"湖景", "天价物价", "准点火车"}, Vibe: "一顿麦当劳吃出法餐价格、湖水清得离谱、火车准点到秒"},
	{ID: "interlaken", Name: "Interlaken", NameZH: "因特拉肯", Country: "Switzerland", CountryZH: "瑞士", Region: "europe", Lat: 46.6863, Lon: 7.8632, Timezone: "Europe/Zurich", Currency: "CHF", AirportCode: "ZRH", Tags: []string{"少女峰", "滑翔伞", "雪山湖泊"}, Vibe: "抬头就是雪山配绿草地像Windows壁纸、超市买面包省钱"},
	{ID: "vienna", Name: "Vienna", NameZH: "维也纳", Country: "Austria", CountryZH: "奥地利", Region: "europe", Lat: 48.2082, Lon: 16.3738, Timezone: "Europe/Vienna", Currency: "EUR", AirportCode: "VIE", Tags: []string{"咖啡馆", "音乐厅", "炸猪排"}, Vibe: "百年老咖啡馆坐一下午、炸猪排比脸还大、周日全城关门"},
	{ID: "prague", Name: "Prague", NameZH: "布拉格", Country: "Czechia", CountryZH: "捷克", Region: "europe", Lat: 50.0755, Lon: 14.4378, Timezone: "Europe/Prague", Currency: "CZK", AirportCode: "PRG", Tags: []string{"红屋顶", "啤酒", "查理大桥"}, Vibe: "啤酒比水便宜、石子路走得脚疼、满城童话红屋顶"},
	{ID: "budapest", Name: "Budapest", NameZH: "布达佩斯", Country: "Hungary", CountryZH: "匈牙利", Region: "europe", Lat: 47.4979, Lon: 19.0402, Timezone: "Europe/Budapest", Currency: "HUF", AirportCode: "BUD", Tags: []string{"温泉", "多瑙河夜景", "炖牛肉"}, Vibe: "在室外温泉池里泡到发晕、多瑙河夜景金色拉满、吃匈牙利炖牛肉"},
	{ID: "berlin", Name: "Berlin", NameZH: "柏林", Country: "Germany", CountryZH: "德国", Region: "europe", Lat: 52.5200, Lon: 13.4050, Timezone: "Europe/Berlin", Currency: "EUR", AirportCode: "BER", Tags: []string{"涂鸦", "烤肉卷", "德铁晚点"}, Vibe: "德铁又双叒晚点了、满街野生涂鸦、半夜吃Döner续命"},
	{ID: "munich", Name: "Munich", NameZH: "慕尼黑", Country: "Germany", CountryZH: "德国", Region: "europe", Lat: 48.1351, Lon: 11.5820, Timezone: "Europe/Berlin", Currency: "EUR", AirportCode: "MUC", Tags: []string{"猪肘", "啤酒花园", "阿尔卑斯"}, Vibe: "一个脆皮猪肘顶一天、公园里大白天全在喝大杯啤酒"},
	{ID: "amsterdam", Name: "Amsterdam", NameZH: "阿姆斯特丹", Country: "Netherlands", CountryZH: "荷兰", Region: "europe", Lat: 52.3676, Lon: 4.9041, Timezone: "Europe/Amsterdam", Currency: "EUR", AirportCode: "AMS", Tags: []string{"运河", "自行车", "薯条"}, Vibe: "过马路小心自行车大军撞飞、运河边歪歪扭扭窄房子、吃现炸大薯条"},
	{ID: "copenhagen", Name: "Copenhagen", NameZH: "哥本哈根", Country: "Denmark", CountryZH: "丹麦", Region: "europe", Lat: 55.6761, Lon: 12.5683, Timezone: "Europe/Copenhagen", Currency: "DKK", AirportCode: "CPH", Tags: []string{"新港", "肉桂卷", "北欧设计"}, Vibe: "风凉得透心、啃个热乎肉桂卷、物价和幸福感一样高"},
	{ID: "stockholm", Name: "Stockholm", NameZH: "斯德哥尔摩", Country: "Sweden", CountryZH: "瑞典", Region: "europe", Lat: 59.3293, Lon: 18.0686, Timezone: "Europe/Stockholm", Currency: "SEK", AirportCode: "ARN", Tags: []string{"群岛", "地铁洞穴", "fika"}, Vibe: "坐地铁像进岩洞美术馆、下午必须Fika喝咖啡、现金几乎没人收"},
	{ID: "oslo", Name: "Oslo", NameZH: "奥斯陆", Country: "Norway", CountryZH: "挪威", Region: "europe", Lat: 59.9139, Lon: 10.7522, Timezone: "Europe/Oslo", Currency: "NOK", AirportCode: "OSL", Tags: []string{"峡湾", "三文鱼", "桑拿"}, Vibe: "海边木屋蒸完桑拿跳冰水、超市三文鱼鲜掉眉毛、空气冷冽"},
	{ID: "tromso", Name: "Tromso", NameZH: "特罗姆瑟", Country: "Norway", CountryZH: "挪威", Region: "polar", Lat: 69.6492, Lon: 18.9553, Timezone: "Europe/Oslo", Currency: "NOK", AirportCode: "TOS", Tags: []string{"北极圈", "极光", "观鲸"}, Vibe: "北极圈里蹲极光冻成冰棍、出海看野生鲸鱼亲戚"},
	{ID: "reykjavik", Name: "Reykjavik", NameZH: "雷克雅未克", Country: "Iceland", CountryZH: "冰岛", Region: "polar", Lat: 64.1466, Lon: -21.9426, Timezone: "Atlantic/Reykjavik", Currency: "ISK", AirportCode: "KEF", Tags: []string{"冰火之岛", "温泉", "妖风", "极光"}, Vibe: "妖风能把车门吹飞、泡地热温泉看雪、吃羊肉热狗取暖"},
	{ID: "helsinki", Name: "Helsinki", NameZH: "赫尔辛基", Country: "Finland", CountryZH: "芬兰", Region: "europe", Lat: 60.1699, Lon: 24.9384, Timezone: "Europe/Helsinki", Currency: "EUR", AirportCode: "HEL", Tags: []string{"社恐天堂", "桑拿", "图书馆"}, Vibe: "等公交车人人间隔两米、安静到舒服、喝奶油三文鱼汤"},
	{ID: "rovaniemi", Name: "Rovaniemi", NameZH: "罗瓦涅米", Country: "Finland", CountryZH: "芬兰", Region: "polar", Lat: 66.5039, Lon: 25.7294, Timezone: "Europe/Helsinki", Currency: "EUR", AirportCode: "RVN", Tags: []string{"圣诞老人村", "北极圈", "驯鹿"}, Vibe: "一脚跨过北极圈白线、雪厚到埋膝盖、偶遇淡定驯鹿"},
	{ID: "athens", Name: "Athens", NameZH: "雅典", Country: "Greece", CountryZH: "希腊", Region: "europe", Lat: 37.9838, Lon: 23.7275, Timezone: "Europe/Athens", Currency: "EUR", AirportCode: "ATH", Tags: []string{"卫城", "烤肉卷", "流浪猫"}, Vibe: "抬头看神庙低头撸猫、大太阳晃眼、吃便宜管饱的Gyros烤肉卷"},
	{ID: "santorini", Name: "Santorini", NameZH: "圣托里尼", Country: "Greece", CountryZH: "希腊", Region: "europe", Lat: 36.3932, Lon: 25.4615, Timezone: "Europe/Athens", Currency: "EUR", AirportCode: "JTR", Tags: []string{"蓝白小屋", "爱琴海", "日落"}, Vibe: "悬崖台阶爬到喘气、海风吹着看爱琴海日落、白墙反光刺眼"},
	{ID: "dubrovnik", Name: "Dubrovnik", NameZH: "杜布罗夫尼克", Country: "Croatia", CountryZH: "克罗地亚", Region: "europe", Lat: 42.6507, Lon: 18.0944, Timezone: "Europe/Zagreb", Currency: "EUR", AirportCode: "DBV", Tags: []string{"君临城", "亚得里亚海", "城墙"}, Vibe: "踩在古城墙上看翡翠海、石阶巷子里钻出好多猫"},
	{ID: "tallinn", Name: "Tallinn", NameZH: "塔林", Country: "Estonia", CountryZH: "爱沙尼亚", Region: "europe", Lat: 59.4370, Lon: 24.7536, Timezone: "Europe/Tallinn", Currency: "EUR", AirportCode: "TLL", Tags: []string{"中世纪老城", "波罗的海", "数字国家"}, Vibe: "中世纪尖塔老城配上超快免费WiFi、海风冷飕飕"},
	{ID: "krakow", Name: "Krakow", NameZH: "克拉科夫", Country: "Poland", CountryZH: "波兰", Region: "europe", Lat: 50.0647, Lon: 19.9450, Timezone: "Europe/Warsaw", Currency: "PLN", AirportCode: "KRK", Tags: []string{"老广场", "波兰饺子", "性价比"}, Vibe: "老广场鸽子不怕人、吃一盘热腾腾波兰饺子、物价在欧洲算良心"},

	// --- North America ---
	{ID: "new_york", Name: "New York", NameZH: "纽约", Country: "USA", CountryZH: "美国", Region: "north_america", Lat: 40.7128, Lon: -74.0060, Timezone: "America/New_York", Currency: "USD", AirportCode: "JFK", Tags: []string{"曼哈顿", "地铁", "披萨", "华尔街"}, Vibe: "路过华尔街顺便看看大盘、一块美元披萨顶饱、地铁老鼠比猫大"},
	{ID: "san_francisco", Name: "San Francisco", NameZH: "旧金山", Country: "USA", CountryZH: "美国", Region: "north_america", Lat: 37.7749, Lon: -122.4194, Timezone: "America/Los_Angeles", Currency: "USD", AirportCode: "SFO", Tags: []string{"硅谷", "金门大桥", "海雾", "大斜坡"}, Vibe: "夏天冷得要穿羽绒服、满街AI广告牌、坐叮当车爬超陡坡"},
	{ID: "los_angeles", Name: "Los Angeles", NameZH: "洛杉矶", Country: "USA", CountryZH: "美国", Region: "north_america", Lat: 34.0522, Lon: -118.2437, Timezone: "America/Los_Angeles", Currency: "USD", AirportCode: "LAX", Tags: []string{"棕榈树", "日落", "堵车", "汉堡"}, Vibe: "没车寸步难行、天天大晴天配棕榈树、吃In-N-Out汉堡"},
	{ID: "seattle", Name: "Seattle", NameZH: "西雅图", Country: "USA", CountryZH: "美国", Region: "north_america", Lat: 47.6062, Lon: -122.3321, Timezone: "America/Los_Angeles", Currency: "USD", AirportCode: "SEA", Tags: []string{"绵绵细雨", "咖啡", "海鲜市场"}, Vibe: "下着毛毛雨没人打伞、派克市场看扔鱼、找家独立咖啡馆发呆"},
	{ID: "chicago", Name: "Chicago", NameZH: "芝加哥", Country: "USA", CountryZH: "美国", Region: "north_america", Lat: 41.8781, Lon: -87.6298, Timezone: "America/Chicago", Currency: "USD", AirportCode: "ORD", Tags: []string{"风城", "深盘披萨", "密歇根湖"}, Vibe: "风大得能吹偏走路路线、深盘披萨腻到顶喉咙、湖大得像海"},
	{ID: "las_vegas", Name: "Las Vegas", NameZH: "拉斯维加斯", Country: "USA", CountryZH: "美国", Region: "north_america", Lat: 36.1699, Lon: -115.1398, Timezone: "America/Los_Angeles", Currency: "USD", AirportCode: "LAS", Tags: []string{"霓虹", "沙漠", "大球"}, Vibe: "半夜三点亮得像白天、室外热到烤人、只看不赌守住钱包"},
	{ID: "miami", Name: "Miami", NameZH: "迈阿密", Country: "USA", CountryZH: "美国", Region: "north_america", Lat: 25.7617, Lon: -80.1918, Timezone: "America/New_York", Currency: "USD", AirportCode: "MIA", Tags: []string{"海滩", "古巴三明治", "热浪"}, Vibe: "满街西语BGM、海滩热辣辣、吃压烤古巴三明治配冰咖啡"},
	{ID: "honolulu", Name: "Honolulu", NameZH: "火奴鲁鲁", Country: "USA", CountryZH: "美国", Region: "oceania", Lat: 21.3069, Lon: -157.8583, Timezone: "Pacific/Honolulu", Currency: "USD", AirportCode: "HNL", Tags: []string{"夏威夷", "彩虹", "冲浪", "Poke"}, Vibe: "抬头动不动就出双彩虹、吃生鱼Poke饭、海风软绵绵"},
	{ID: "anchorage", Name: "Anchorage", NameZH: "安克雷奇", Country: "USA", CountryZH: "美国", Region: "polar", Lat: 61.2181, Lon: -149.9003, Timezone: "America/Anchorage", Currency: "USD", AirportCode: "ANC", Tags: []string{"阿拉斯加", "冰川", "荒野"}, Vibe: "路边真能撞见驼鹿、冷风呼呼吹、吃帝王蟹和三文鱼"},
	{ID: "vancouver", Name: "Vancouver", NameZH: "温哥华", Country: "Canada", CountryZH: "加拿大", Region: "north_america", Lat: 49.2827, Lon: -123.1207, Timezone: "America/Vancouver", Currency: "CAD", AirportCode: "YVR", Tags: []string{"雨哥华", "中餐神地", "海山相接"}, Vibe: "雨下个没完、列治文中餐卷过国内、海边散步看水上飞机"},
	{ID: "toronto", Name: "Toronto", NameZH: "多伦多", Country: "Canada", CountryZH: "加拿大", Region: "north_america", Lat: 43.6532, Lon: -79.3832, Timezone: "America/Toronto", Currency: "CAD", AirportCode: "YYZ", Tags: []string{"CN塔", "湖风", "多元美食"}, Vibe: "安大略湖边风挺硬、地下城逛晕、什么国家菜都能吃到"},
	{ID: "montreal", Name: "Montreal", NameZH: "蒙特利尔", Country: "Canada", CountryZH: "加拿大", Region: "north_america", Lat: 45.5017, Lon: -73.5673, Timezone: "America/Toronto", Currency: "CAD", AirportCode: "YUL", Tags: []string{"法语区", "熏肉三明治", "肉汁薯条"}, Vibe: "路牌全是法语、吃热乎Poutine配熏肉、老港像小欧洲"},
	{ID: "banff", Name: "Banff", NameZH: "班夫", Country: "Canada", CountryZH: "加拿大", Region: "north_america", Lat: 51.1784, Lon: -115.5708, Timezone: "America/Edmonton", Currency: "CAD", AirportCode: "YYC", Tags: []string{"落基山", "冰川湖", "松林"}, Vibe: "湖水绿得像颜料桶倒进去、满山松香味、早晚冷到缩脖子"},
	{ID: "mexico_city", Name: "Mexico City", NameZH: "墨西哥城", Country: "Mexico", CountryZH: "墨西哥", Region: "north_america", Lat: 19.4326, Lon: -99.1332, Timezone: "America/Mexico_City", Currency: "MXN", AirportCode: "MEX", Tags: []string{"Taco", "高原", "蓝花楹"}, Vibe: "海拔两千二走快了有点喘、路边摊Taco便宜到哭、满街涂鸦壁画"},
	{ID: "cancun", Name: "Cancun", NameZH: "坎昆", Country: "Mexico", CountryZH: "墨西哥", Region: "north_america", Lat: 21.1619, Lon: -86.8515, Timezone: "America/Cancun", Currency: "MXN", AirportCode: "CUN", Tags: []string{"加勒比海", "天然井", "玛雅"}, Vibe: "加勒比海蓝得发亮、跳进凉丝丝的天然井游泳、躺平吹海风"},

	// --- South America ---
	{ID: "rio", Name: "Rio de Janeiro", NameZH: "里约热内卢", Country: "Brazil", CountryZH: "巴西", Region: "south_america", Lat: -22.9068, Lon: -43.1729, Timezone: "America/Sao_Paulo", Currency: "BRL", AirportCode: "GIG", Tags: []string{"沙滩排球", "烤肉", "山海"}, Vibe: "海滩全员松弛感拉满、吃巴西烤肉配冰果汁、手机死死攥手里"},
	{ID: "buenos_aires", Name: "Buenos Aires", NameZH: "布宜诺斯艾利斯", Country: "Argentina", CountryZH: "阿根廷", Region: "south_america", Lat: -34.6037, Lon: -58.3816, Timezone: "America/Argentina/Buenos_Aires", Currency: "ARS", AirportCode: "EZE", Tags: []string{"牛排", "探戈", "南美巴黎"}, Vibe: "跟北京刚好差11小时彻底昼夜颠倒、牛排厚到惊人、老书店发呆"},
	{ID: "ushuaia", Name: "Ushuaia", NameZH: "乌斯怀亚", Country: "Argentina", CountryZH: "阿根廷", Region: "polar", Lat: -54.8019, Lon: -68.3030, Timezone: "America/Argentina/Ushuaia", Currency: "ARS", AirportCode: "USH", Tags: []string{"世界尽头", "南极门户", "帝王蟹"}, Vibe: "真到了世界尽头、风冷得像南极直吹、吃一顿新鲜火地岛帝王蟹"},
	{ID: "santiago", Name: "Santiago", NameZH: "圣地亚哥", Country: "Chile", CountryZH: "智利", Region: "south_america", Lat: -33.4489, Lon: -70.6693, Timezone: "America/Santiago", Currency: "CLP", AirportCode: "SCL", Tags: []string{"安第斯雪山", "车厘子", "红酒"}, Vibe: "抬头就能看见安第斯雪山、水果甜度超标、跟国内时差十二小时"},
	{ID: "lima", Name: "Lima", NameZH: "利马", Country: "Peru", CountryZH: "秘鲁", Region: "south_america", Lat: -12.0464, Lon: -77.0428, Timezone: "America/Lima", Currency: "PEN", AirportCode: "LIM", Tags: []string{"酸橘汁腌鱼", "悬崖海岸", "常年无雨"}, Vibe: "海雾蒙蒙几乎不下雨、吃酸爽的Ceviche生腌鱼、悬崖公园看滑翔伞"},
	{ID: "cusco", Name: "Cusco", NameZH: "库斯科", Country: "Peru", CountryZH: "秘鲁", Region: "south_america", Lat: -13.5319, Lon: -71.9675, Timezone: "America/Lima", Currency: "PEN", AirportCode: "CUZ", Tags: []string{"马丘比丘", "羊驼", "高原"}, Vibe: "海拔三千四走两步喘粗气、街边跟羊驼对视、喝古柯叶茶缓高反"},
	{ID: "medellin", Name: "Medellin", NameZH: "麦德林", Country: "Colombia", CountryZH: "哥伦比亚", Region: "south_america", Lat: 6.2476, Lon: -75.5658, Timezone: "America/Bogota", Currency: "COP", AirportCode: "MDE", Tags: []string{"永恒之春", "咖啡", "缆车"}, Vibe: "常年二十多度舒服到不想走、坐城市缆车俯瞰山谷、喝现磨单品咖啡"},

	// --- Africa ---
	{ID: "cape_town", Name: "Cape Town", NameZH: "开普敦", Country: "South Africa", CountryZH: "南非", Region: "africa", Lat: -33.9249, Lon: 18.4241, Timezone: "Africa/Johannesburg", Currency: "ZAR", AirportCode: "CPT", Tags: []string{"桌山", "企鹅滩", "好望角"}, Vibe: "海滩上看小企鹅摇摇摆摆、桌山云像桌布铺着、好望角风大到飞起"},
	{ID: "marrakech", Name: "Marrakech", NameZH: "马拉喀什", Country: "Morocco", CountryZH: "摩洛哥", Region: "africa", Lat: 31.6295, Lon: -7.9811, Timezone: "Africa/Casablanca", Currency: "MAD", AirportCode: "RAK", Tags: []string{"老城迷宫", "薄荷茶", "塔吉锅"}, Vibe: "麦地那老巷彻底走晕、满城红土墙配薄荷茶、跟摊主砍价砍到口干"},
	{ID: "nairobi", Name: "Nairobi", NameZH: "内罗毕", Country: "Kenya", CountryZH: "肯尼亚", Region: "africa", Lat: -1.2921, Lon: 36.8219, Timezone: "Africa/Nairobi", Currency: "KES", AirportCode: "NBO", Tags: []string{"草原动物", "长颈鹿", "肯尼亚咖啡"}, Vibe: "市区边上就能看野生动物、早晚意外凉爽、咖啡豆香气炸裂"},
	{ID: "zanzibar", Name: "Zanzibar", NameZH: "桑给巴尔", Country: "Tanzania", CountryZH: "坦桑尼亚", Region: "africa", Lat: -6.1659, Lon: 39.2026, Timezone: "Africa/Dar_es_Salaam", Currency: "TZS", AirportCode: "ZNZ", Tags: []string{"香料岛", "印度洋", "石头城"}, Vibe: "印度洋海水清透、石头城空气里都是丁香肉桂味、退潮踩白沙"},

	// --- Oceania ---
	{ID: "sydney", Name: "Sydney", NameZH: "悉尼", Country: "Australia", CountryZH: "澳大利亚", Region: "oceania", Lat: -33.8688, Lon: 151.2093, Timezone: "Australia/Sydney", Currency: "AUD", AirportCode: "SYD", Tags: []string{"歌剧院", "海港渡轮", "澳白咖啡"}, Vibe: "坐渡轮吹海港风、喝一口丝滑Flat White、海鸥虎视眈眈抢薯条"},
	{ID: "melbourne", Name: "Melbourne", NameZH: "墨尔本", Country: "Australia", CountryZH: "澳大利亚", Region: "oceania", Lat: -37.8136, Lon: 144.9631, Timezone: "Australia/Melbourne", Currency: "AUD", AirportCode: "MEL", Tags: []string{"一天四季", "咖啡巷", "免费电车"}, Vibe: "一小时内经历晴天暴雨妖风、涂鸦小巷喝咖啡、坐免费电车发呆"},
	{ID: "auckland", Name: "Auckland", NameZH: "奥克兰", Country: "New Zealand", CountryZH: "新西兰", Region: "oceania", Lat: -36.8509, Lon: 174.7645, Timezone: "Pacific/Auckland", Currency: "NZD", AirportCode: "AKL", Tags: []string{"千帆之都", "草坪发呆", "南半球"}, Vibe: "满港白帆船、空气干净到呛人、下午五点商店就关门准备躺平"},
	{ID: "queenstown", Name: "Queenstown", NameZH: "皇后镇", Country: "New Zealand", CountryZH: "新西兰", Region: "oceania", Lat: -45.0312, Lon: 168.6626, Timezone: "Pacific/Auckland", Currency: "NZD", AirportCode: "ZQN", Tags: []string{"雪山湖景", "大汉堡", "极限运动"}, Vibe: "排半小时买个比脸大的Fergburger、看别人蹦极自己坚决不上"},
	{ID: "nadi", Name: "Fiji", NameZH: "斐济", Country: "Fiji", CountryZH: "斐济", Region: "oceania", Lat: -17.7765, Lon: 177.4356, Timezone: "Pacific/Fiji", Currency: "FJD", AirportCode: "NAN", Tags: []string{"南太平洋", "Fiji Time", "珊瑚海"}, Vibe: "全岛执行Fiji Time主打一个慢慢来、泡在南太平洋海水里摆烂"},
}

// countryDefaultCity maps country names (ZH/EN) to an iconic default city when users suggest a country.
var countryDefaultCity = map[string]string{
	"日本": "tokyo", "japan": "tokyo",
	"韩国": "seoul", "korea": "seoul", "south korea": "seoul",
	"泰国": "bangkok", "thailand": "bangkok",
	"越南": "hanoi", "vietnam": "hanoi",
	"马来西亚": "kuala_lumpur", "大马": "kuala_lumpur", "malaysia": "kuala_lumpur",
	"印尼": "bali", "印度尼西亚": "bali", "indonesia": "bali",
	"冰岛": "reykjavik", "iceland": "reykjavik",
	"挪威": "tromso", "norway": "oslo",
	"芬兰": "rovaniemi", "finland": "helsinki",
	"瑞典": "stockholm", "sweden": "stockholm",
	"丹麦": "copenhagen", "denmark": "copenhagen",
	"英国": "london", "uk": "london",
	"法国": "paris", "france": "paris",
	"德国": "berlin", "germany": "berlin",
	"意大利": "rome", "italy": "rome",
	"瑞士": "interlaken", "switzerland": "interlaken",
	"西班牙": "barcelona", "spain": "barcelona",
	"葡萄牙": "lisbon", "portugal": "lisbon",
	"荷兰": "amsterdam", "netherlands": "amsterdam",
	"奥地利": "vienna", "austria": "vienna",
	"捷克": "prague", "czech": "prague",
	"匈牙利": "budapest", "hungary": "budapest",
	"希腊": "santorini", "greece": "athens",
	"土耳其": "istanbul", "turkey": "istanbul",
	"埃及": "cairo", "egypt": "cairo",
	"摩洛哥": "marrakech", "morocco": "marrakech",
	"南非": "cape_town", "south africa": "cape_town",
	"肯尼亚": "nairobi", "kenya": "nairobi",
	"美国": "new_york", "usa": "new_york", "us": "new_york",
	"夏威夷": "honolulu", "hawaii": "honolulu",
	"阿拉斯加": "anchorage", "alaska": "anchorage",
	"加拿大": "vancouver", "canada": "vancouver",
	"墨西哥": "mexico_city", "mexico": "mexico_city",
	"巴西": "rio", "brazil": "rio",
	"阿根廷": "buenos_aires", "argentina": "buenos_aires",
	"南极": "ushuaia", "世界尽头": "ushuaia",
	"智利": "santiago", "chile": "santiago",
	"秘鲁": "cusco", "peru": "lima",
	"哥伦比亚": "medellin", "colombia": "medellin",
	"澳大利亚": "sydney", "澳洲": "sydney", "australia": "sydney",
	"新西兰": "queenstown", "new zealand": "queenstown",
	"尼泊尔": "kathmandu", "nepal": "kathmandu",
	"印度": "jaipur", "india": "jaipur",
	"格鲁吉亚": "tbilisi", "georgia": "tbilisi",
	"乌兹别克斯坦": "samarkand", "uzbekistan": "samarkand",
	"哈萨克斯坦": "almaty", "kazakhstan": "almaty",
	"蒙古": "ulaanbaatar", "mongolia": "ulaanbaatar",
	"爱尔兰": "dublin", "ireland": "dublin",
	"波兰": "krakow", "poland": "krakow",
	"克罗地亚": "dubrovnik", "croatia": "dubrovnik",
	"爱沙尼亚": "tallinn", "estonia": "tallinn",
	"斯里兰卡": "colombo", "sri lanka": "colombo",
	"柬埔寨": "siem_reap", "cambodia": "siem_reap",
	"菲律宾": "manila", "philippines": "manila",
	"约旦": "amman", "jordan": "amman",
	"阿联酋": "dubai", "uae": "dubai",
	"卡塔尔": "doha", "qatar": "doha",
}

// AllCities returns a copy of the built-in global city atlas.
func AllCities() []model.TravelCity {
	out := make([]model.TravelCity, len(builtinCities))
	copy(out, builtinCities)
	return out
}

// CityByID looks up a city by exact canonical ID.
func CityByID(id string) (model.TravelCity, bool) {
	id = strings.ToLower(strings.TrimSpace(id))
	for _, c := range builtinCities {
		if c.ID == id {
			return c, true
		}
	}
	return model.TravelCity{}, false
}

// FindCity resolves a city from ID, IATA airport code, Chinese/English city name, or country alias.
func FindCity(raw string) (model.TravelCity, bool) {
	q := strings.TrimSpace(raw)
	if q == "" {
		return model.TravelCity{}, false
	}
	ql := strings.ToLower(q)

	// 1. Exact ID / IATA / Name / NameZH
	for _, c := range builtinCities {
		if c.ID == ql || strings.ToLower(c.AirportCode) == ql || strings.ToLower(c.Name) == ql || c.NameZH == q {
			return c, true
		}
	}
	// 2. Country alias exact match
	if cid, ok := countryDefaultCity[q]; ok {
		return CityByID(cid)
	}
	if cid, ok := countryDefaultCity[ql]; ok {
		return CityByID(cid)
	}
	// 3. Substring match on city name or Chinese name
	for _, c := range builtinCities {
		if strings.Contains(q, c.NameZH) || strings.Contains(c.NameZH, q) ||
			strings.Contains(ql, strings.ToLower(c.Name)) || strings.Contains(strings.ToLower(c.Name), ql) {
			return c, true
		}
	}
	// 4. Substring match on country name
	for _, c := range builtinCities {
		if strings.Contains(q, c.CountryZH) || strings.Contains(ql, strings.ToLower(c.Country)) {
			return c, true
		}
	}
	return model.TravelCity{}, false
}

// HaversineKm returns the great-circle distance in kilometers between two coordinates.
func HaversineKm(lat1, lon1, lat2, lon2 float64) float64 {
	const earthR = 6371.0
	dLat := (lat2 - lat1) * math.Pi / 180.0
	dLon := (lon2 - lon1) * math.Pi / 180.0
	rLat1 := lat1 * math.Pi / 180.0
	rLat2 := lat2 * math.Pi / 180.0
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(rLat1)*math.Cos(rLat2)*math.Sin(dLon/2)*math.Sin(dLon/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	return earthR * c
}

var islandCityIDs = map[string]bool{
	"taipei": true, "naha": true, "jeju": true, "bali": true, "phuket": true,
	"male": true, "colombo": true, "manila": true, "reykjavik": true, "dublin": true,
	"santorini": true, "honolulu": true, "zanzibar": true, "auckland": true,
	"queenstown": true, "nadi": true,
}

// EstimateTransit calculates a realistic transport mode ("train" | "ferry" | "flight"),
// travel duration, and great-circle distance between two cities.
func EstimateTransit(from, to model.TravelCity) (mode string, dur time.Duration, distKm float64) {
	distKm = HaversineKm(from.Lat, from.Lon, to.Lat, to.Lon)
	if distKm < 1 {
		return "train", 45 * time.Minute, 0
	}

	// Short sea crossing or rail-friendly corridor
	sameMainlandRail := from.Region == to.Region &&
		!islandCityIDs[from.ID] && !islandCityIDs[to.ID] &&
		(from.Region == "east_asia" || from.Region == "europe") &&
		(from.Country == to.Country || from.Region == "europe")

	switch {
	case sameMainlandRail && distKm <= 950:
		mode = "train"
		hours := distKm/190.0 + 0.75
		if hours < 1.2 {
			hours = 1.2
		}
		dur = time.Duration(math.Round(hours*60)) * time.Minute
	case distKm <= 280 && (islandCityIDs[from.ID] || islandCityIDs[to.ID]) && from.Country == to.Country:
		mode = "ferry"
		hours := distKm/55.0 + 0.8
		dur = time.Duration(math.Round(hours*60)) * time.Minute
	default:
		mode = "flight"
		hours := distKm/810.0 + 1.8 // includes boarding/taxi/buffer
		if distKm > 8500 {
			hours += 1.5 // layover buffer on ultra-long-haul
		}
		if hours < 1.8 {
			hours = 1.8
		}
		if hours > 17.5 {
			hours = 17.5
		}
		dur = time.Duration(math.Round(hours*60)) * time.Minute
	}
	return mode, dur, math.Round(distKm)
}

func transportModeZH(mode string) string {
	switch mode {
	case "train":
		return "高铁/火车"
	case "ferry":
		return "渡轮"
	default:
		return "航班"
	}
}

func loadCityLocation(tz string) *time.Location {
	if loc, err := time.LoadLocation(tz); err == nil {
		return loc
	}
	return time.UTC
}
