/**
 * 位图标签工具类
 * 使用位图来管理account标签，替换散列的布尔字段
 */

// 标签位图定义
const TAG_BITS = {
  EXCHANGE: 1 << 0,     // 1<<0 EX（Exchange）
  SMART_MONEY: 1 << 1,  // 1<<1 SM（SmartMoney）
  WHALE: 1 << 2,        // 1<<2 WH（Whale）
  PUBLIC_FIGURE: 1 << 3, // 1<<3 PF（PublicFigure）
  FRESH: 1 << 4,        // 1<<4 FR（Fresh）
  TOP_PNL: 1 << 5       // 1<<5 TP（TopPnL）
};

// 标签名称映射（用于兼容现有的字符串标签）
const TAG_NAME_TO_BIT = {
  'cex': TAG_BITS.EXCHANGE,
  'exchange': TAG_BITS.EXCHANGE,
  'smart': TAG_BITS.SMART_MONEY,
  'whale': TAG_BITS.WHALE,
  'fresh': TAG_BITS.FRESH,
  'public': TAG_BITS.PUBLIC_FIGURE,
  'top_pnl': TAG_BITS.TOP_PNL,
  'normal': 0  // normal标签对应位图值为0
};

// 位图值到标签名称的映射
const BIT_TO_TAG_NAME = {
  [TAG_BITS.EXCHANGE]: 'cex',
  [TAG_BITS.SMART_MONEY]: 'smart',
  [TAG_BITS.WHALE]: 'whale',
  [TAG_BITS.PUBLIC_FIGURE]: 'public',
  [TAG_BITS.FRESH]: 'fresh',
  [TAG_BITS.TOP_PNL]: 'top_pnl'
};

/**
 * 将字符串标签转换为位图值
 * @param {string} tagString - 标签字符串（如'cex', 'smart_money'等）
 * @returns {number} 位图值
 */
function tagStringToBitmap(tagString) {
  if (!tagString || typeof tagString !== 'string') {
    return 0;
  }
  
  const normalizedTag = tagString.toLowerCase().trim();
  return TAG_NAME_TO_BIT[normalizedTag] || 0;
}

/**
 * 将位图值转换为主要标签字符串
 * @param {number} bitmap - 位图值
 * @returns {string} 主要标签字符串，如果没有标签则返回'normal'
 */
function bitmapToTagString(bitmap) {
  if (!bitmap || bitmap === 0) {
    return 'normal';
  }
  
  // 返回第一个匹配的标签（优先级：exchange > smart_money > whale > fresh > public_figure > top_pnl）
  for (const bit of [TAG_BITS.EXCHANGE, TAG_BITS.SMART_MONEY, TAG_BITS.WHALE, TAG_BITS.FRESH, TAG_BITS.PUBLIC_FIGURE, TAG_BITS.TOP_PNL]) {
    if (bitmap & bit) {
      return BIT_TO_TAG_NAME[bit];
    }
  }
  
  return 'normal';
}

/**
 * 将位图值转换为所有标签数组
 * @param {number} bitmap - 位图值
 * @returns {string[]} 标签字符串数组
 */
function bitmapToTagArray(bitmap) {
  if (!bitmap || bitmap === 0) {
    return ['normal'];
  }
  
  const tags = [];
  for (const [bit, tagName] of Object.entries(BIT_TO_TAG_NAME)) {
    if (bitmap & parseInt(bit)) {
      tags.push(tagName);
    }
  }
  
  return tags.length > 0 ? tags : ['normal'];
}

/**
 * 检查位图是否包含特定标签
 * @param {number} bitmap - 位图值
 * @param {string} tagString - 要检查的标签字符串
 * @returns {boolean} 是否包含该标签
 */
function hasTag(bitmap, tagString) {
  const bit = tagStringToBitmap(tagString);
  return (bitmap & bit) !== 0;
}

/**
 * 向位图添加标签
 * @param {number} bitmap - 原位图值
 * @param {string} tagString - 要添加的标签字符串
 * @returns {number} 新的位图值
 */
function addTag(bitmap, tagString) {
  const bit = tagStringToBitmap(tagString);
  return bitmap | bit;
}

/**
 * 从位图移除标签
 * @param {number} bitmap - 原位图值
 * @param {string} tagString - 要移除的标签字符串
 * @returns {number} 新的位图值
 */
function removeTag(bitmap, tagString) {
  const bit = tagStringToBitmap(tagString);
  return bitmap & (~bit);
}

/**
 * 兼容性函数：检查是否为CEX标签
 * @param {number} bitmap - 位图值
 * @returns {boolean}
 */
function isCexTag(bitmap) {
  return hasTag(bitmap, 'cex');
}

/**
 * 兼容性函数：检查是否为SmartMoney标签
 * @param {number} bitmap - 位图值
 * @returns {boolean}
 */
function isSmartMoneyTag(bitmap) {
  return hasTag(bitmap, 'smart');
}

/**
 * 兼容性函数：检查是否为Whale标签
 * @param {number} bitmap - 位图值
 * @returns {boolean}
 */
function isWhaleTag(bitmap) {
  return hasTag(bitmap, 'whale');
}

/**
 * 兼容性函数：检查是否为Fresh标签
 * @param {number} bitmap - 位图值
 * @returns {boolean}
 */
function isFreshTag(bitmap) {
  return hasTag(bitmap, 'fresh');
}

module.exports = {
  TAG_BITS,
  TAG_NAME_TO_BIT,
  BIT_TO_TAG_NAME,
  tagStringToBitmap,
  bitmapToTagString,
  bitmapToTagArray,
  hasTag,
  addTag,
  removeTag,
  isCexTag,
  isSmartMoneyTag,
  isWhaleTag,
  isFreshTag
};

