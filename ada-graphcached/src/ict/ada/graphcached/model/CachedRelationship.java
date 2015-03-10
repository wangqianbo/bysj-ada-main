/**
 * 
 */
package ict.ada.graphcached.model;

/**
 * @author forhappy
 *
 */
public class CachedRelationship {
  private String relationshipId;

  /**
   * @return the relationshipId
   */
  public String getRelationshipId() {
    return relationshipId;
  }

  /**
   * @param relationshipId the relationshipId to set
   */
  public void setRelationshipId(String relationshipId) {
    this.relationshipId = relationshipId;
  }

  /**
   * @param relationshipId
   */
  public CachedRelationship(String relationshipId) {
    super();
    this.relationshipId = relationshipId;
  }
  
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (!(o instanceof CachedRelationship))
      return false;

    @SuppressWarnings("unchecked")
    final CachedRelationship cachedRelationship = (CachedRelationship) o;

    if (relationshipId != null ? !relationshipId.equals(cachedRelationship.getRelationshipId()) : cachedRelationship.getRelationshipId() != null)
      return false;

    return true;
  }

  public int hashCode() {
    int result;
    result = (relationshipId != null ? relationshipId.hashCode() : 0);
    return result;
  }

  public String toString() {
    return relationshipId;
  }
  
}
